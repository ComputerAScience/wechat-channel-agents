import { query, type Options, type SDKAssistantMessage, type SDKResultMessage } from "@anthropic-ai/claude-agent-sdk";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { homedir } from "node:os";
import type { AgentBackend, AgentRequest, AgentResponse } from "../interface.js";
import type { AppConfig } from "../../types.js";
import { getSession, updateSession } from "../../storage/sessions.js";
import { createHooks } from "./hooks.js";
import { logger } from "../../util/logger.js";

/** Read the current access token from ~/.claude/.credentials.json.
 *  Falls back to the config token if the file is missing or unreadable.
 *  This ensures we always use the latest token even after automatic refresh. */
function readCurrentAuthToken(fallback: string): string {
  try {
    const credPath = join(homedir(), ".claude", ".credentials.json");
    const cred = JSON.parse(readFileSync(credPath, "utf-8"));
    const token = cred?.claudeAiOauth?.accessToken;
    if (token) return token;
  } catch {
    // file missing or invalid – fall back to config value
  }
  return fallback;
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

export class ClaudeBackend implements AgentBackend {
  readonly type = "claude" as const;

  constructor(private config: AppConfig) {}

  async run(req: AgentRequest): Promise<AgentResponse> {
    const session = getSession(req.userId);
    const toolsUsed: string[] = [];
    let resultText = "";
    let sessionId = "";
    let isError = false;

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
        ANTHROPIC_AUTH_TOKEN: readCurrentAuthToken(this.config.anthropicAuthToken),
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
          case "assistant": {
            const text = extractText(msg);
            if (text) {
              lastAssistantTexts.push(text);
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
