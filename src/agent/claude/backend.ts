import { query, type Options, type SDKAssistantMessage, type SDKResultMessage } from "@anthropic-ai/claude-agent-sdk";
import { readFileSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { homedir } from "node:os";
import type { AgentBackend, AgentRequest, AgentResponse } from "../interface.js";
import type { AppConfig } from "../../types.js";
import { getSession, updateSession } from "../../storage/sessions.js";
import { createHooks } from "./hooks.js";
import { logger } from "../../util/logger.js";

const OAUTH_TOKEN_URL = "https://platform.claude.com/v1/oauth/token";
/** Refresh token 提前 5 分钟触发 */
const REFRESH_BEFORE_EXPIRY_MS = 5 * 60 * 1000;

interface ClaudeCredentials {
  claudeAiOauth: {
    accessToken: string;
    refreshToken?: string;
    expiresAt?: number;
  };
}

/** 读取 ~/.claude/.credentials.json，必要时用 refreshToken 自动刷新 accessToken */
async function getValidAuthToken(fallback: string): Promise<string> {
  const credPath = join(homedir(), ".claude", ".credentials.json");

  let creds: ClaudeCredentials;
  try {
    creds = JSON.parse(readFileSync(credPath, "utf-8")) as ClaudeCredentials;
  } catch {
    logger.debug("Could not read credentials file, using fallback token");
    return fallback;
  }

  const { accessToken, refreshToken, expiresAt } = creds.claudeAiOauth ?? {};
  if (!accessToken) return fallback;

  // token 未过期（或没有过期时间），直接返回
  const now = Date.now();
  if (!expiresAt || expiresAt - now > REFRESH_BEFORE_EXPIRY_MS) {
    return accessToken;
  }

  // token 即将过期，尝试用 refreshToken 刷新
  if (!refreshToken) {
    logger.warn("OAuth token expired and no refreshToken available");
    return accessToken;
  }

  logger.info("OAuth token expiring soon, refreshing...");
  try {
    const resp = await fetch(OAUTH_TOKEN_URL, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ grant_type: "refresh_token", refresh_token: refreshToken }),
    });

    if (!resp.ok) {
      logger.error(`Token refresh failed: ${resp.status} ${await resp.text()}`);
      return accessToken;
    }

    const data = (await resp.json()) as {
      access_token: string;
      refresh_token?: string;
      expires_in?: number;
    };

    const newCreds: ClaudeCredentials = {
      claudeAiOauth: {
        ...creds.claudeAiOauth,
        accessToken: data.access_token,
        refreshToken: data.refresh_token ?? refreshToken,
        expiresAt: data.expires_in ? now + data.expires_in * 1000 : undefined,
      },
    };

    writeFileSync(credPath, JSON.stringify(newCreds), "utf-8");
    logger.info("OAuth token refreshed successfully");
    return data.access_token;
  } catch (err) {
    logger.error(`Token refresh error: ${String(err)}`);
    return accessToken;
  }
}

function extractText(msg: SDKAssistantMessage): string {
  const parts: string[] = [];
  for (const block of msg.message.content) {
    if (block.type === "text") {
      parts.push(block.text);
    }
  }
  return parts.join("");
}

function extractStreamDelta(msg: { event: unknown }): string {
  const event = msg.event as {
    type?: string;
    delta?: {
      type?: string;
      text?: string;
    };
  };

  if (
    event?.type === "content_block_delta" &&
    event.delta?.type === "text_delta" &&
    typeof event.delta.text === "string"
  ) {
    return event.delta.text;
  }

  return "";
}

export class ClaudeBackend implements AgentBackend {
  readonly type = "claude" as const;

  constructor(private config: AppConfig) {}

  async run(req: AgentRequest): Promise<AgentResponse> {
    const session = getSession(req.userId);
    const toolsUsed: string[] = [];
    let resultText = "";
    let sessionId = "";
    let isError = false;
    let streamedText = "";
    let sawPartialStream = false;

    const options: Options = {
      cwd: req.cwd,
      allowedTools: [
        "Read", "Edit", "Write", "Bash", "Grep", "Glob",
        "WebSearch", "WebFetch", "Agent",
      ],
      permissionMode: "bypassPermissions",
      hooks: createHooks(),
      env: {
        ...process.env,
        ANTHROPIC_BASE_URL: this.config.anthropicBaseUrl,
        ANTHROPIC_AUTH_TOKEN: await getValidAuthToken(this.config.anthropicAuthToken),
      },
      maxTurns: 30,
    };

    // Resume existing session if available
    if (session?.claudeSessionId) {
      options.resume = session.claudeSessionId;
      logger.debug(`Resuming Claude session for user=${req.userId}`);
    }

    try {
      const stream = query({ prompt: req.prompt, options });
      const lastAssistantTexts: string[] = [];

      for await (const msg of stream) {
        sessionId = msg.session_id;

        switch (msg.type) {
          case "stream_event": {
            const delta = extractStreamDelta(msg);
            if (delta) {
              sawPartialStream = true;
              streamedText += delta;
              await req.onTextDelta?.(delta);
            }
            break;
          }

          case "assistant": {
            const text = extractText(msg);
            if (text) {
              lastAssistantTexts.push(text);
              if (!sawPartialStream && text.startsWith(streamedText)) {
                const delta = text.slice(streamedText.length);
                if (delta) {
                  streamedText = text;
                  await req.onTextDelta?.(delta);
                }
              }
            }
            for (const block of msg.message.content) {
              if (block.type === "tool_use") {
                toolsUsed.push(block.name);
              }
            }
            break;
          }

          case "result": {
            const result = msg as SDKResultMessage;
            isError = result.is_error;
            if (result.subtype === "success") {
              resultText = result.result;
            }
            break;
          }
        }
      }

      if (!resultText && lastAssistantTexts.length > 0) {
        resultText = lastAssistantTexts[lastAssistantTexts.length - 1];
      }

      // Save session for future resume
      if (sessionId) {
        updateSession(req.userId, { claudeSessionId: sessionId });
      }

      return {
        text: resultText || "(No response)",
        isError,
        toolsUsed,
      };
    } catch (err) {
      logger.error(`Claude agent query failed: user=${req.userId} err=${String(err)}`);
      return {
        text: `Error: ${String(err)}`,
        isError: true,
        toolsUsed,
      };
    }
  }

  resetSession(userId: string): void {
    updateSession(userId, { claudeSessionId: undefined });
    logger.info(`Claude session reset for user=${userId}`);
  }

  getStatus(userId: string): string {
    const session = getSession(userId);
    if (session?.claudeSessionId) {
      return `Claude session: ${session.claudeSessionId.slice(0, 8)}...`;
    }
    return "Claude: no active session";
  }
}
