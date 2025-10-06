import type { Request } from "express";
import type WebSocket from "ws";
// plane imports
import { Controller, WebSocket as WSDecorator } from "@plane/decorators";
import { logger } from "@plane/logger";
// redis
import { redisManager } from "@/redis";
// auth
import { handleAuthentication } from "@/lib/auth";

@Controller("/issues")
export class IssueEventsController {
  @WSDecorator("/")
  async handleConnection(ws: WebSocket, req: Request) {
    const query = req.query as Record<string, string | string[]>;

    const projectIdRaw = query.projectId;
    const workspaceSlugRaw = query.workspaceSlug;
    const tokenRaw = query.token;

    const projectId = Array.isArray(projectIdRaw) ? projectIdRaw[0] : projectIdRaw;
    const workspaceSlug = Array.isArray(workspaceSlugRaw) ? workspaceSlugRaw[0] : workspaceSlugRaw;
    const token = Array.isArray(tokenRaw) ? tokenRaw[0] : tokenRaw;

    if (!projectId || !workspaceSlug || !token) {
      ws.close(4001, "Missing required parameters");
      return;
    }

    let parsedToken: { id?: string; cookie?: string } | undefined;
    try {
      parsedToken = JSON.parse(token);
    } catch (error) {
      logger.error("Invalid token payload for issue events", error);
      ws.close(4002, "Invalid token");
      return;
    }

    const cookieString = parsedToken?.cookie || req.headers.cookie || "";
    if (cookieString) {
      try {
        await handleAuthentication({
          cookie: cookieString,
          userId: parsedToken?.id ?? "",
        });
      } catch (error) {
        logger.error("Failed to authenticate issue events connection", error);
        ws.close(4003, "Unauthorized");
        return;
      }
    } else if (!parsedToken?.id) {
      ws.close(4003, "Unauthorized");
      return;
    }

    const redisClient = redisManager.getClient();
    if (!redisClient) {
      ws.close(1011, "Realtime service unavailable");
      return;
    }

    const channel = `issue_events:${projectId}`;
    const subscriber = redisClient.duplicate();
    let cleanupInitiated = false;

    const cleanup = async () => {
      if (cleanupInitiated) return;
      cleanupInitiated = true;
      subscriber.removeAllListeners("message");
      subscriber.removeAllListeners("error");
      try {
        await subscriber.unsubscribe(channel);
      } catch (error) {
        logger.error("Failed to unsubscribe issue events channel", error);
      }
      subscriber.disconnect();
    };

    try {
      subscriber.on("error", (error) => {
        logger.error("Issue events redis subscriber error", error);
        if (ws.readyState === ws.OPEN || ws.readyState === ws.CONNECTING) {
          ws.close(1011, "Realtime service unavailable");
        }
        void cleanup();
      });
      await subscriber.connect();
      subscriber.on("message", (incomingChannel, message) => {
        if (incomingChannel === channel && ws.readyState === ws.OPEN) {
          ws.send(message);
        }
      });
      await subscriber.subscribe(channel);
      ws.on("close", () => {
        void cleanup();
      });
      ws.on("error", (error) => {
        logger.error("Issue events websocket error", error);
        if (ws.readyState === ws.OPEN || ws.readyState === ws.CONNECTING) {
          ws.close(1011, "Issue events websocket error");
        }
        void cleanup();
      });
    } catch (error) {
      logger.error("Failed to subscribe to issue events channel", error);
      if (ws.readyState === ws.OPEN || ws.readyState === ws.CONNECTING) {
        ws.close(1011, "Subscription failure");
      }
      void cleanup();
    }
  }
}
