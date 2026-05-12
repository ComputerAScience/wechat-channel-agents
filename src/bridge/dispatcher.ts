import { writeFile, unlink, access, readFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, extname } from "node:path";
import type { WeixinMessage, MessageItem } from "../wechat/types.js";
import { MessageType, MessageItemType, TypingStatus } from "../wechat/types.js";
import { sendTyping } from "../wechat/api.js";
import type { WeixinApiOptions } from "../wechat/api.js";
import { sendTextMessage, markdownToPlainText } from "../wechat/send.js";
import { sendImage, sendFile } from "../wechat/send-media.js";
import { downloadImage, downloadFile, downloadVideo } from "../media/download.js";
import { setContextToken, getContextToken } from "../wechat/context-token.js";
import { getAgent, getRegisteredTypes } from "../agent/registry.js";
import { getOrCreateSession, updateSession, resetAgentSession } from "../storage/sessions.js";
import { listCodexCliSessions, resolveCodexHome } from "../storage/external-sessions.js";
import type { ExternalSession } from "../storage/external-sessions.js";
import { hasAdminUsers, isUserAdmin, isUserAllowed } from "../auth/allowlist.js";
import { resolveAvailableAgentType } from "./agent-resolution.js";
import { formatResponse, toolUseSummary } from "./formatter.js";
import { chunkText } from "./chunker.js";
import { createStreamingSender } from "./streaming-sender.js";
import { logger } from "../util/logger.js";
import { redactUserId } from "../util/redact.js";
import { buildConversationKey, type AgentType, type AppConfig } from "../types.js";

const TYPING_INTERVAL_MS = 10_000;
const STREAM_FLUSH_INTERVAL_MS = 2_000;
const STREAM_FLUSH_CHARS = 300;

// Regex to detect image/file paths in Claude's response text
const OUTGOING_IMAGE_REGEX = /((?:\/|~\/)[^\s"'`，。！？,!?]+\.(?:png|jpg|jpeg|gif|webp|bmp))/gi;
const OUTGOING_FILE_REGEX = /((?:\/|~\/)[^\s"'`，。！？,!?]+\.(?:pdf|doc|docx|xls|xlsx|csv|txt|zip))/gi;

/** Save a buffer to a temp file, returns the temp path */
async function saveMediaToTemp(data: Buffer, ext: string): Promise<string> {
  const tmpPath = join(tmpdir(), `wechat-${Date.now()}-${Math.random().toString(36).slice(2)}.${ext}`);
  await writeFile(tmpPath, data);
  return tmpPath;
}

/** Download all media items from an incoming message, save to temp files.
 *  Returns a prompt suffix describing the attachments and a list of temp paths to clean up. */
async function buildMediaPrompt(mediaItems: MessageItem[]): Promise<{ promptSuffix: string; tempFiles: string[] }> {
  const tempFiles: string[] = [];
  const lines: string[] = [];

  for (const item of mediaItems) {
    try {
      if (item.type === MessageItemType.IMAGE && item.image_item) {
        const data = await downloadImage(item.image_item);
        if (data) {
          const tmpPath = await saveMediaToTemp(data, "jpg");
          tempFiles.push(tmpPath);
          lines.push(`[用户发送了一张图片，已保存至 ${tmpPath}，请用 Read 工具查看并分析]`);
        }
      } else if (item.type === MessageItemType.FILE && item.file_item) {
        const data = await downloadFile(item.file_item);
        if (data) {
          const fileName = item.file_item.file_name ?? `file_${Date.now()}`;
          const ext = extname(fileName).replace(".", "") || "bin";
          const tmpPath = await saveMediaToTemp(data, ext);
          tempFiles.push(tmpPath);
          lines.push(`[用户发送了文件 "${fileName}"，已保存至 ${tmpPath}，请用 Read 工具查看]`);
        }
      } else if (item.type === MessageItemType.VIDEO && item.video_item) {
        const data = await downloadVideo(item.video_item);
        if (data) {
          const tmpPath = await saveMediaToTemp(data, "mp4");
          tempFiles.push(tmpPath);
          lines.push(`[用户发送了视频，已保存至 ${tmpPath}]`);
        }
      }
    } catch (err) {
      logger.error(`Failed to download media item type=${item.type}: ${String(err)}`);
    }
  }

  return { promptSuffix: lines.join("\n"), tempFiles };
}

/** Scan Claude's response text for image/file paths and send them back to the user via WeChat. */
async function sendMediaFromResponse(
  accountId: string,
  apiOpts: WeixinApiOptions,
  userId: string,
  responseText: string,
): Promise<void> {
  const home = process.env.HOME ?? "";

  const imagePaths = [...new Set([...(responseText.match(OUTGOING_IMAGE_REGEX) ?? [])])];
  logger.info(`sendMediaFromResponse: found ${imagePaths.length} image path(s): ${imagePaths.join(", ")}`);
  for (const rawPath of imagePaths) {
    const filePath = rawPath.startsWith("~/") ? rawPath.replace("~", home) : rawPath;
    try {
      await access(filePath);
      const data = await readFile(filePath);
      logger.info(`Sending image size=${data.length} path=${filePath}`);
      await sendImage(apiOpts, accountId, userId, data);
      logger.info(`Sent image to user=${redactUserId(userId)} path=${filePath}`);
    } catch (err) {
      logger.error(`Failed to send image path=${filePath}: ${String(err)}`);
    }
  }

  const filePaths = [...new Set([...(responseText.match(OUTGOING_FILE_REGEX) ?? [])])];
  logger.info(`sendMediaFromResponse: found ${filePaths.length} file path(s): ${filePaths.join(", ")}`);
  for (const rawPath of filePaths) {
    const filePath = rawPath.startsWith("~/") ? rawPath.replace("~", home) : rawPath;
    const fileName = filePath.split("/").pop() ?? "file";
    try {
      await access(filePath);
      const data = await readFile(filePath);
      logger.info(`Sending file size=${data.length} name=${fileName} path=${filePath}`);
      await sendFile(apiOpts, accountId, userId, data, fileName);
      logger.info(`Sent file to user=${redactUserId(userId)} path=${filePath}`);
    } catch (err) {
      logger.error(`Failed to send file path=${filePath}: ${String(err)}`);
    }
  }
}

export interface DispatcherDeps {
  config: AppConfig;
  onLogout?: () => Promise<void>;
  onLogin?: () => Promise<{ accountId: string }>;
  listAccounts?: () => string[];
}

export function createDispatcher(deps: DispatcherDeps) {
  const { config } = deps;

  return async function dispatch(params: {
    accountId: string;
    apiOpts: WeixinApiOptions;
    msg: WeixinMessage;
    typingTicket: string;
  }): Promise<void> {
    const { accountId, apiOpts, msg, typingTicket } = params;

    // Only process USER messages
    if (msg.message_type !== MessageType.USER) return;

    const userId = msg.from_user_id;
    if (!userId) return;
    const conversationKey = buildConversationKey(accountId, userId);

    // Cache context_token
    if (msg.context_token) {
      setContextToken(accountId, userId, msg.context_token);
    }

    // Extract text + media
    const { text, mediaItems } = extractContent(msg);
    if (!text && mediaItems.length === 0) return;

    // Allowlist check
    if (!isUserAllowed(userId)) {
      logger.warn(`User not in allowlist: ${redactUserId(userId)}`);
      return;
    }

    logger.info(`Message from=${redactUserId(userId)} len=${text.length} media=${mediaItems.length}`);

    // Parse commands
    const trimmed = text.trim();
    const firstWord = trimmed.split(/\s/)[0].toLowerCase();

    switch (firstWord) {
      case "/claude":
        await handleSwitch(accountId, apiOpts, userId, conversationKey, "claude");
        return;
      case "/codex":
        await handleSwitch(accountId, apiOpts, userId, conversationKey, "codex");
        return;
      case "/reset":
        await handleReset(accountId, apiOpts, userId, conversationKey);
        return;
      case "/status":
        await handleStatus(accountId, apiOpts, userId, conversationKey);
        return;
      case "/help":
        await handleHelp(accountId, apiOpts, userId);
        return;
      case "/sessions":
        await handleSessions(accountId, apiOpts, userId, trimmed.slice(9).trim());
        return;
      case "/resume":
        await handleResume(accountId, apiOpts, userId, conversationKey, trimmed.slice(7).trim());
        return;
      case "/cwd":
        await handleCwd(accountId, apiOpts, userId, conversationKey, trimmed.slice(4).trim());
        return;
      case "/sendfile":
      case "/sendimage":
        await handleSendFile(accountId, apiOpts, userId, trimmed.split(/\s+/).slice(1).join(" ").trim());
        return;
      case "/login":
        await handleLogin(accountId, apiOpts, userId);
        return;
      case "/logout":
        await handleLogout(accountId, apiOpts, userId);
        return;
    }

    // Route to agent
    const session = getOrCreateSession(conversationKey, config.defaultAgent, config.codex.workingDirectory);
    const agentType = ensureSessionAgentAvailable(conversationKey, userId, session);
    let streamedText = "";
    const sender = createStreamingSender({
      send: (chunk) => sendChunkSafely(accountId, apiOpts, userId, chunk),
      flushIntervalMs: STREAM_FLUSH_INTERVAL_MS,
      flushChars: STREAM_FLUSH_CHARS,
      maxChunkLen: config.textChunkLimit,
    });

    // Start typing indicator
    const typingController = new AbortController();
    startTypingLoop(apiOpts, userId, typingTicket, typingController.signal);

    try {
      // Download incoming media attachments and build prompt suffix
      const { promptSuffix, tempFiles } = await buildMediaPrompt(mediaItems);
      const fullPrompt = promptSuffix ? `${trimmed}\n${promptSuffix}`.trim() : trimmed;

      const agent = getAgent(agentType);
      const result = await agent.run({
        userId: conversationKey,
        prompt: fullPrompt,
        cwd: session.cwd,
        onTextDelta: async (text) => {
          if (!text) return;
          streamedText += text;
          await sender.push(text);
        },
      });

      typingController.abort();

      if (streamedText) {
        await sender.finish(buildStreamingFinalTail(
          userId,
          result.text,
          streamedText,
          result.toolsUsed,
          result.isError,
        ));
      } else {
        const response = formatResponse(result.text, result.toolsUsed, result.isError);
        const plainText = markdownToPlainText(response);
        const chunks = chunkText(plainText, config.textChunkLimit);
        await sendChunks(accountId, apiOpts, userId, chunks);
      }

      // Send any image/file paths found in Claude's response back to the user
      await sendMediaFromResponse(accountId, apiOpts, userId, result.text);

      // Clean up temp files created from incoming media
      await Promise.allSettled(tempFiles.map((f) => unlink(f)));
    } catch (err) {
      typingController.abort();
      logger.error(`Agent error for user=${redactUserId(userId)}: ${String(err)}`);
      if (streamedText) {
        await sender.finish();
      }
      await sendReply(accountId, apiOpts, userId, `Error: ${String(err)}`);
    }
  };

  async function handleSwitch(
    accountId: string,
    apiOpts: WeixinApiOptions,
    userId: string,
    conversationKey: string,
    agentType: AgentType,
  ): Promise<void> {
    const types = getRegisteredTypes();
    if (!types.includes(agentType)) {
      await sendReply(accountId, apiOpts, userId, `Agent "${agentType}" is not available. Available: ${types.join(", ")}`);
      return;
    }
    const session = getOrCreateSession(conversationKey, config.defaultAgent, config.codex.workingDirectory);
    const currentAgentType = ensureSessionAgentAvailable(conversationKey, userId, session);
    if (currentAgentType === agentType) {
      await sendReply(accountId, apiOpts, userId, `Already using ${agentType}.`);
      return;
    }
    updateSession(conversationKey, { agentType });
    await sendReply(
      accountId,
      apiOpts,
      userId,
      `Switched to ${agentType}. Previous ${currentAgentType} session is preserved.`,
    );
  }

  async function handleReset(
    accountId: string,
    apiOpts: WeixinApiOptions,
    userId: string,
    conversationKey: string,
  ): Promise<void> {
    const session = getOrCreateSession(conversationKey, config.defaultAgent, config.codex.workingDirectory);
    const agentType = ensureSessionAgentAvailable(conversationKey, userId, session);
    const agent = getAgent(agentType);
    agent.resetSession(conversationKey);
    resetAgentSession(conversationKey, agentType);
    await sendReply(accountId, apiOpts, userId, `${agentType} session reset. Starting fresh.`);
  }

  function getCodexSessionList(): ExternalSession[] {
    return listCodexCliSessions(config.codex.home);
  }

  async function handleSessions(
    accountId: string,
    apiOpts: WeixinApiOptions,
    userId: string,
    arg: string,
  ): Promise<void> {
    const normalizedArg = arg.trim().toLowerCase();
    if (normalizedArg && normalizedArg !== "codex") {
      await sendReply(accountId, apiOpts, userId, "Usage: /sessions codex");
      return;
    }

    const sessions = getCodexSessionList();
    const codexHome = resolveCodexHome(config.codex.home);
    if (sessions.length === 0) {
      await sendReply(accountId, apiOpts, userId, `No Codex sessions found in ${codexHome}/sessions`);
      return;
    }

    const lines = [
      `Codex sessions from ${codexHome}:`,
      ...sessions.map((session, index) => {
        const time = new Date(session.modifiedAt).toISOString();
        const cwd = session.cwd || "(unknown cwd)";
        return `${index + 1}. ${session.id.slice(0, 8)}... ${session.project} ${time}\n   ${cwd}`;
      }),
      "",
      "Use /resume codex <n> to restore one.",
    ];
    await sendReply(accountId, apiOpts, userId, lines.join("\n"));
  }

  async function handleResume(
    accountId: string,
    apiOpts: WeixinApiOptions,
    userId: string,
    conversationKey: string,
    arg: string,
  ): Promise<void> {
    const parts = arg.split(/\s+/).filter(Boolean);
    if (parts.length !== 2 || parts[0].toLowerCase() !== "codex") {
      await sendReply(accountId, apiOpts, userId, "Usage: /resume codex <n>\nUse /sessions codex to see available sessions.");
      return;
    }

    const index = Number.parseInt(parts[1], 10);
    const sessions = getCodexSessionList();
    const target = Number.isInteger(index) ? sessions[index - 1] : undefined;
    if (!target) {
      await sendReply(accountId, apiOpts, userId, `Codex session #${parts[1]} not found. Use /sessions codex to see available sessions.`);
      return;
    }

    const current = getOrCreateSession(conversationKey, config.defaultAgent, config.codex.workingDirectory);
    const cwd = target.cwd || current.cwd || config.codex.workingDirectory;
    updateSession(conversationKey, {
      agentType: "codex",
      codexThreadId: target.id,
      cwd,
    });

    await sendReply(
      accountId,
      apiOpts,
      userId,
      `Resumed Codex session ${target.id.slice(0, 8)}... cwd: ${cwd}`,
    );
  }

  async function handleStatus(
    accountId: string,
    apiOpts: WeixinApiOptions,
    userId: string,
    conversationKey: string,
  ): Promise<void> {
    const session = getOrCreateSession(conversationKey, config.defaultAgent, config.codex.workingDirectory);
    const agentType = ensureSessionAgentAvailable(conversationKey, userId, session);
    const agent = getAgent(agentType);
    const agentStatus = agent.getStatus(conversationKey);
    const lines = [
      `Current bot account: ${accountId}`,
      `Connected bot accounts: ${deps.listAccounts?.().join(", ") ?? accountId}`,
      `Current agent: ${agentType}`,
      `CWD: ${session.cwd}`,
      `Last active: ${new Date(session.lastActive).toISOString()}`,
      agentStatus,
    ];
    await sendReply(accountId, apiOpts, userId, lines.join("\n"));
  }

  async function handleHelp(accountId: string, apiOpts: WeixinApiOptions, userId: string): Promise<void> {
    const types = getRegisteredTypes();
    const loginHelp = hasAdminUsers()
      ? "  /login - Add another bot account by QR login (admin only)"
      : "  /login - Add another bot account by QR login (disabled until adminUsers is configured)";
    const logoutHelp = hasAdminUsers()
      ? "  /logout - Log out all bot accounts and stop service (admin only)"
      : "  /logout - Log out all bot accounts and stop service (disabled until adminUsers is configured)";
    const lines = [
      "Commands:",
      ...types.map((t) => `  /${t} - Switch to ${t}`),
      "  /reset - Reset current agent session",
      "  /status - Show current status",
      "  /help - Show this help",
      "  /sessions codex - List Codex sessions from CODEX_HOME",
      "  /resume codex <n> - Resume a Codex session from the list",
      "  /cwd <path> - Change working directory",
      "  /sendfile <path> - Send a file/image directly to yourself",
      loginHelp,
      logoutHelp,
      "",
      `Available agents: ${types.join(", ")}`,
      `Current bot account: ${accountId}`,
      "Send any text to chat with the current agent.",
    ];
    await sendReply(accountId, apiOpts, userId, lines.join("\n"));
  }

  async function handleCwd(
    accountId: string,
    apiOpts: WeixinApiOptions,
    userId: string,
    conversationKey: string,
    newCwd: string,
  ): Promise<void> {
    const session = getOrCreateSession(conversationKey, config.defaultAgent, config.codex.workingDirectory);
    if (!newCwd) {
      await sendReply(accountId, apiOpts, userId, `Current CWD: ${session.cwd}`);
    } else {
      updateSession(conversationKey, { cwd: newCwd });
      await sendReply(accountId, apiOpts, userId, `Working directory changed to: ${newCwd}`);
    }
  }

  async function handleLogin(accountId: string, apiOpts: WeixinApiOptions, userId: string): Promise<void> {
    if (!hasAdminUsers()) {
      logger.warn(`Login command denied for user=${redactUserId(userId)}: no admin users configured`);
      await sendReply(accountId, apiOpts, userId, "Command /login is disabled until adminUsers is configured.");
      return;
    }

    if (!isUserAdmin(userId)) {
      logger.warn(`Login command denied for non-admin user=${redactUserId(userId)}`);
      await sendReply(accountId, apiOpts, userId, "Command /login is restricted to admin users.");
      return;
    }

    if (!deps.onLogin) {
      await sendReply(accountId, apiOpts, userId, "Account login is not available in this runtime.");
      return;
    }

    await sendReply(
      accountId,
      apiOpts,
      userId,
      "Starting QR login for an additional bot account. Check the terminal to scan the QR code.",
    );

    try {
      const result = await deps.onLogin();
      await sendReply(
        accountId,
        apiOpts,
        userId,
        `Additional bot account connected: ${result.accountId}`,
      );
    } catch (err) {
      await sendReply(
        accountId,
        apiOpts,
        userId,
        `Failed to add bot account: ${String(err)}`,
      );
    }
  }

  async function handleSendFile(
    accountId: string,
    apiOpts: WeixinApiOptions,
    userId: string,
    filePath: string,
  ): Promise<void> {
    if (!filePath) {
      await sendReply(
        accountId,
        apiOpts,
        userId,
        "用法: /sendfile <路径>\n示例: /sendfile /mnt/c/Users/lymor/Desktop/1.png",
      );
      return;
    }

    const home = process.env.HOME ?? "";
    const resolvedPath = filePath.startsWith("~/") ? filePath.replace("~", home) : filePath;
    const ext = resolvedPath.split(".").pop()?.toLowerCase() ?? "";
    const imageExts = ["png", "jpg", "jpeg", "gif", "webp", "bmp"];

    logger.info(`handleSendFile: path=${resolvedPath} user=${redactUserId(userId)}`);

    try {
      const data = await readFile(resolvedPath);
      if (imageExts.includes(ext)) {
        await sendImage(apiOpts, accountId, userId, data);
        logger.info(`handleSendFile: sent image size=${data.length} to=${redactUserId(userId)}`);
      } else {
        const fileName = resolvedPath.split("/").pop() ?? "file";
        await sendFile(apiOpts, accountId, userId, data, fileName);
        logger.info(`handleSendFile: sent file name=${fileName} size=${data.length} to=${redactUserId(userId)}`);
      }
      await sendReply(accountId, apiOpts, userId, `✅ 已发送: ${resolvedPath}`);
    } catch (err) {
      logger.error(`handleSendFile failed path=${resolvedPath}: ${String(err)}`);
      await sendReply(accountId, apiOpts, userId, `❌ 发送失败: ${String(err)}`);
    }
  }

  async function handleLogout(accountId: string, apiOpts: WeixinApiOptions, userId: string): Promise<void> {
    if (!hasAdminUsers()) {
      logger.warn(`Logout command denied for user=${redactUserId(userId)}: no admin users configured`);
      await sendReply(accountId, apiOpts, userId, "Command /logout is disabled until adminUsers is configured.");
      return;
    }

    if (!isUserAdmin(userId)) {
      logger.warn(`Logout command denied for non-admin user=${redactUserId(userId)}`);
      await sendReply(accountId, apiOpts, userId, "Command /logout is restricted to admin users.");
      return;
    }

    await sendReply(
      accountId,
      apiOpts,
      userId,
      "Logging out all bot accounts. Local credentials will be cleared and the service will stop. Restart npm run dev or use /login after restart to scan a new QR code.",
    );

    await deps.onLogout?.();
  }

  async function sendReply(
    accountId: string,
    apiOpts: WeixinApiOptions,
    userId: string,
    text: string,
  ): Promise<void> {
    const contextToken = getContextToken(accountId, userId);
    if (!contextToken) {
      logger.error(`No contextToken for accountId=${accountId} user=${redactUserId(userId)}, cannot send reply`);
      return;
    }
    const chunks = chunkText(text, config.textChunkLimit);
    await sendChunks(accountId, apiOpts, userId, chunks);
  }

  async function sendChunks(
    accountId: string,
    apiOpts: WeixinApiOptions,
    userId: string,
    chunks: string[],
  ): Promise<void> {
    const contextToken = getContextToken(accountId, userId);
    for (const chunk of chunks) {
      try {
        await sendTextMessage({
          to: userId,
          text: chunk,
          opts: { ...apiOpts, contextToken },
        });
      } catch (err) {
        logger.error(`Failed to send chunk accountId=${accountId} to=${redactUserId(userId)}: ${String(err)}`);
      }
      if (chunks.length > 1) {
        await new Promise((r) => setTimeout(r, 200));
      }
    }
  }

  async function sendChunkSafely(
    accountId: string,
    apiOpts: WeixinApiOptions,
    userId: string,
    text: string,
  ): Promise<void> {
    try {
      await sendChunks(accountId, apiOpts, userId, [text]);
    } catch (err) {
      logger.error(`Failed to stream chunk accountId=${accountId} to=${redactUserId(userId)}: ${String(err)}`);
    }
  }

  function buildStreamingFinalTail(
    userId: string,
    finalText: string,
    streamedText: string,
    toolsUsed: string[],
    isError: boolean,
  ): string {
    const parts: string[] = [];
    if (finalText.startsWith(streamedText)) {
      const remaining = finalText.slice(streamedText.length);
      if (remaining) {
        parts.push(remaining);
      }
    } else if (finalText !== streamedText) {
      logger.warn(`Final streamed text mismatch for user=${redactUserId(userId)}; appending final body`);
      parts.push(finalText);
    }

    const summary = toolUseSummary(toolsUsed);
    if (summary) {
      parts.push(summary);
    }

    if (isError) {
      if (parts.length > 0) {
        parts[0] = `[Error] ${parts[0]}`;
      } else {
        parts.push("[Error]");
      }
    }

    return parts.join("\n\n");
  }

  function startTypingLoop(
    apiOpts: WeixinApiOptions,
    userId: string,
    ticket: string,
    signal: AbortSignal,
  ): void {
    const sendTypingOnce = async () => {
      try {
        await sendTyping({
          baseUrl: apiOpts.baseUrl,
          token: apiOpts.token,
          routeTag: apiOpts.routeTag,
          body: {
            ilink_user_id: userId,
            typing_ticket: ticket,
            status: TypingStatus.TYPING,
          },
        });
      } catch {
        // Typing failures are silently ignored
      }
    };

    void sendTypingOnce();

    const interval = setInterval(() => {
      if (signal.aborted) {
        clearInterval(interval);
        return;
      }
      void sendTypingOnce();
    }, TYPING_INTERVAL_MS);

    signal.addEventListener("abort", () => clearInterval(interval), { once: true });
  }

  function ensureSessionAgentAvailable(
    conversationKey: string,
    userId: string,
    session: { agentType: AgentType },
  ): AgentType {
    const resolvedAgentType = resolveAvailableAgentType(
      session.agentType,
      config.defaultAgent,
      getRegisteredTypes(),
    );

    if (resolvedAgentType !== session.agentType) {
      logger.warn(
        `Session agent ${session.agentType} unavailable for user=${redactUserId(userId)}; falling back to ${resolvedAgentType}`,
      );
      updateSession(conversationKey, { agentType: resolvedAgentType });
    }

    return resolvedAgentType;
  }
}

function extractContent(msg: WeixinMessage): { text: string; mediaItems: MessageItem[] } {
  if (!msg.item_list?.length) return { text: "", mediaItems: [] };
  let text = "";
  const mediaItems: MessageItem[] = [];
  for (const item of msg.item_list) {
    if (item.type === MessageItemType.TEXT && item.text_item?.text) {
      text = item.text_item.text;
    } else if (
      item.type === MessageItemType.IMAGE ||
      item.type === MessageItemType.FILE ||
      item.type === MessageItemType.VIDEO
    ) {
      mediaItems.push(item);
    }
  }
  return { text, mediaItems };
}
