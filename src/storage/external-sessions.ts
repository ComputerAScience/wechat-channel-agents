import fs from "node:fs";
import path from "node:path";
import os from "node:os";
import { logger } from "../util/logger.js";

export interface ExternalSession {
  id: string;
  project: string;
  cwd: string;
  modifiedAt: number;
}

export function resolveCodexHome(configuredHome?: string): string {
  return configuredHome || process.env.CODEX_HOME || path.join(os.homedir(), ".codex");
}

export function listCodexCliSessions(configuredHome?: string, limit = 20): ExternalSession[] {
  const sessionsDir = path.join(resolveCodexHome(configuredHome), "sessions");
  const results: ExternalSession[] = [];

  try {
    if (!fs.existsSync(sessionsDir)) return results;
    walkCodexSessions(sessionsDir, results);
  } catch (err) {
    logger.warn(`Failed to scan Codex CLI sessions: ${String(err)}`);
  }

  results.sort((a, b) => b.modifiedAt - a.modifiedAt);
  return results.slice(0, limit);
}

function walkCodexSessions(dir: string, results: ExternalSession[]): void {
  for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
    const fullPath = path.join(dir, entry.name);
    if (entry.isDirectory()) {
      walkCodexSessions(fullPath, results);
      continue;
    }

    if (!entry.name.startsWith("rollout-") || !entry.name.endsWith(".jsonl")) continue;

    const threadId = extractCodexThreadId(entry.name.replace(".jsonl", ""));
    if (!threadId) continue;

    let cwd = "";
    try {
      const firstLine = fs.readFileSync(fullPath, "utf-8").split("\n", 1)[0];
      const meta = JSON.parse(firstLine);
      if (meta?.type === "session_meta" && typeof meta?.payload?.cwd === "string") {
        cwd = meta.payload.cwd;
      }
    } catch {
      // Older or partial rollout files can still be listed without cwd metadata.
    }

    try {
      const stat = fs.statSync(fullPath);
      results.push({
        id: threadId,
        project: cwd ? path.basename(cwd.replace(/\\/g, "/")) : "unknown",
        cwd,
        modifiedAt: stat.mtimeMs,
      });
    } catch {
      // Skip unreadable files.
    }
  }
}

function extractCodexThreadId(baseName: string): string | null {
  const match = baseName.match(/([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})$/);
  return match ? match[1] : null;
}
