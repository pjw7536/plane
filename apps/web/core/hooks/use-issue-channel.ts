"use client";

import { useEffect, useRef } from "react";
// plane imports
import { LIVE_BASE_PATH, LIVE_BASE_URL } from "@plane/constants";
import { useUser } from "@/hooks/store/user";
// root store
import { rootStore } from "@/lib/store-context";

const MAX_RECONNECT_INTERVAL = 30000;
const INITIAL_RECONNECT_INTERVAL = 1000;

const buildWsUrl = (workspaceSlug: string, projectId: string, token: string) => {
  const baseUrl = LIVE_BASE_URL?.trim() || window.location.origin;
  const url = new URL(baseUrl);
  const isSecure = url.protocol === "https:" || window.location.protocol === "https:";
  url.protocol = isSecure ? "wss:" : "ws:";
  url.pathname = `${LIVE_BASE_PATH}/issues`;
  url.searchParams.set("workspaceSlug", workspaceSlug);
  url.searchParams.set("projectId", projectId);
  url.searchParams.set("token", token);
  return url.toString();
};

type TIssueEventPayload = {
  actor_id?: string | null;
  issue_id: string;
  project_id: string;
  timestamp?: number;
  type: string;
  requested_data?: Record<string, any> | string | null;
  current_instance?: Record<string, any> | string | null;
};

type IssueSyncState = {
  latestPayload: TIssueEventPayload | null;
  pendingShouldUpdateGroupedLists: boolean;
  isSyncing: boolean;
};

export const useIssueChannel = (workspaceSlug?: string, projectId?: string) => {
  const { data: currentUser } = useUser();
  const websocketRef = useRef<WebSocket | null>(null);
  const reconnectTimeoutRef = useRef<number | undefined>(undefined);
  const reconnectAttemptRef = useRef(0);
  const issueSyncStateRef = useRef<Map<string, IssueSyncState>>(new Map());

  useEffect(() => {
    if (!workspaceSlug || !projectId || !currentUser?.id) return;

    let isShuttingDown = false;
    const resolvedWorkspaceSlug = workspaceSlug;
    const resolvedProjectId = projectId;

    const closeExistingConnection = () => {
      if (websocketRef.current) {
        websocketRef.current.close();
        websocketRef.current = null;
      }
      if (reconnectTimeoutRef.current) {
        window.clearTimeout(reconnectTimeoutRef.current);
        reconnectTimeoutRef.current = undefined;
      }
    };

    const resetIssueSyncState = () => {
      issueSyncStateRef.current.clear();
    };

    const issueRequiresGroupedListUpdate = (payload: TIssueEventPayload) =>
      payload.type.startsWith("issue.") ||
      payload.type.startsWith("module.") ||
      payload.type.startsWith("cycle.") ||
      payload.type.startsWith("issue_relation.") ||
      payload.type.startsWith("issue_vote.") ||
      payload.type.startsWith("issue_reaction.");

    const processIssueSync = async (issueId: string) => {
      if (isShuttingDown) return;

      const state = issueSyncStateRef.current.get(issueId);
      if (!state || !state.latestPayload) {
        issueSyncStateRef.current.delete(issueId);
        return;
      }

      const payload = state.latestPayload;
      const shouldUpdateGroupedLists = state.pendingShouldUpdateGroupedLists;
      state.pendingShouldUpdateGroupedLists = false;
      issueSyncStateRef.current.set(issueId, state);

      const projectIssuesStore = rootStore.issue.projectIssues;
      const moduleIssuesStore = rootStore.issue.moduleIssues;
      const existingIssue = rootStore.issue.issues.getIssueById(issueId);
      const activeModuleId = rootStore.issue.moduleId;
      const wasInActiveModule = Boolean(
        activeModuleId && Array.isArray(existingIssue?.module_ids) && existingIssue?.module_ids?.includes(activeModuleId)
      );

      if (shouldUpdateGroupedLists) {
        projectIssuesStore?.removeIssueFromList(issueId);
      }

      if (wasInActiveModule) {
        moduleIssuesStore?.removeIssueFromList(issueId);
      }

      try {
        const fetchedIssues = await rootStore.issue.issues.getIssues(
          resolvedWorkspaceSlug,
          resolvedProjectId,
          [issueId]
        );
        const issue = fetchedIssues?.[0];

        if (!issue) {
          projectIssuesStore?.removeIssueFromList(issueId);
          moduleIssuesStore?.removeIssueFromList(issueId);
          rootStore.issue.issues.removeIssue(issueId);
          return;
        }

        if (isShuttingDown) return;

        rootStore.issue.issues.addIssue([issue]);

        if (shouldUpdateGroupedLists) {
          projectIssuesStore?.addIssueToList(issue.id);
        }

        if (activeModuleId) {
          const isInActiveModuleNow = Array.isArray(issue.module_ids) && issue.module_ids.includes(activeModuleId);

          if (isInActiveModuleNow) {
            moduleIssuesStore?.addIssueToList(issue.id);
          }
          // No additional removal call needed when the issue leaves the active module, since we already
          // removed it using the previous module state before fetching the latest issue details.
        }
      } catch (error: any) {
        if (isShuttingDown) return;
        const status = error?.response?.status ?? error?.status;
        if (status === 404) {
          projectIssuesStore?.removeIssueFromList(issueId);
          rootStore.issue.issues.removeIssue(issueId);
          moduleIssuesStore?.removeIssueFromList(issueId);
        } else {
          console.error("Failed to process realtime issue event", error);
        }
      } finally {
        if (isShuttingDown) {
          issueSyncStateRef.current.delete(issueId);
          return;
        }

        const currentState = issueSyncStateRef.current.get(issueId);
        if (!currentState) return;

        const hasNewPayload = currentState.latestPayload !== payload;
        const hasPendingListUpdate = currentState.pendingShouldUpdateGroupedLists;

        if (hasNewPayload || hasPendingListUpdate) {
          currentState.isSyncing = true;
          issueSyncStateRef.current.set(issueId, currentState);
          Promise.resolve().then(() => processIssueSync(issueId));
        } else {
          currentState.isSyncing = false;
          currentState.latestPayload = null;
          currentState.pendingShouldUpdateGroupedLists = false;
          issueSyncStateRef.current.delete(issueId);
        }
      }
    };

    const scheduleIssueSync = (payload: TIssueEventPayload) => {
      if (!payload?.issue_id || isShuttingDown) return;

      const state = issueSyncStateRef.current.get(payload.issue_id) ?? {
        latestPayload: null,
        pendingShouldUpdateGroupedLists: false,
        isSyncing: false,
      };

      state.latestPayload = payload;
      state.pendingShouldUpdateGroupedLists =
        state.pendingShouldUpdateGroupedLists || issueRequiresGroupedListUpdate(payload);
      issueSyncStateRef.current.set(payload.issue_id, state);

      if (!state.isSyncing) {
        state.isSyncing = true;
        issueSyncStateRef.current.set(payload.issue_id, state);
        void processIssueSync(payload.issue_id);
      }
    };

    const connect = () => {
      const token = JSON.stringify({ id: currentUser.id, cookie: document.cookie });
      const wsUrl = buildWsUrl(resolvedWorkspaceSlug, resolvedProjectId, token);
      const ws = new WebSocket(wsUrl);
      websocketRef.current = ws;

      ws.onopen = () => {
        reconnectAttemptRef.current = 0;
      };

      ws.onmessage = (event) => {
        try {
          const data = JSON.parse(event.data as string) as TIssueEventPayload;
          if (data.project_id === resolvedProjectId) {
            scheduleIssueSync(data);
          }
        } catch (error) {
          console.error("Failed to parse issue event payload", error);
        }
      };

      ws.onerror = () => {
        ws.close();
      };

      ws.onclose = () => {
        websocketRef.current = null;
        if (isShuttingDown) return;
        const attempt = reconnectAttemptRef.current + 1;
        reconnectAttemptRef.current = attempt;
        const delay = Math.min(
          MAX_RECONNECT_INTERVAL,
          INITIAL_RECONNECT_INTERVAL * Math.pow(2, attempt - 1)
        );
        reconnectTimeoutRef.current = window.setTimeout(connect, delay);
      };
    };

    connect();

    return () => {
      isShuttingDown = true;
      closeExistingConnection();
      resetIssueSyncState();
    };
  }, [workspaceSlug, projectId, currentUser?.id]);
};
