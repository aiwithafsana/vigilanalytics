"use client";
/**
 * useAlertSocket — real-time fraud alert hook.
 *
 * Connects to /api/ws?token=<jwt> and exposes:
 *   unreadCount   — total unseen alert count since last clearCount()
 *   latestAlerts  — the most recent batch of alerts from the server
 *   clearCount    — call when the user visits /alerts to reset the badge
 *   connected     — true while the WebSocket is open
 *
 * Reconnects automatically with exponential back-off (1s → 2s → 4s … max 30s).
 * Stops reconnecting when the component unmounts (e.g. user logs out).
 */
import { useCallback, useEffect, useRef, useState } from "react";

const BASE_WS =
  (process.env.NEXT_PUBLIC_API_URL ?? "http://localhost:8000").replace(
    /^http/,
    "ws"
  );

export interface AlertItem {
  flag_id: number;
  npi: string;
  provider_name: string;
  specialty: string | null;
  state: string | null;
  risk_score: number;
  flag_type: string;
  severity: number;
  explanation: string | null;
  estimated_overpayment: number | null;
  created_at: string | null;
}

export interface UseAlertSocketResult {
  unreadCount: number;
  latestAlerts: AlertItem[];
  clearCount: () => void;
  connected: boolean;
}

export function useAlertSocket(): UseAlertSocketResult {
  const [unreadCount, setUnreadCount] = useState(0);
  const [latestAlerts, setLatestAlerts] = useState<AlertItem[]>([]);
  const [connected, setConnected] = useState(false);

  const wsRef = useRef<WebSocket | null>(null);
  const retryTimer = useRef<ReturnType<typeof setTimeout> | null>(null);
  const retryDelay = useRef(1_000); // ms; doubles each failure, capped at 30s
  const dead = useRef(false); // set to true on cleanup to stop reconnects

  const clearCount = useCallback(() => setUnreadCount(0), []);

  useEffect(() => {
    dead.current = false;

    function connect() {
      if (dead.current) return;

      const token =
        typeof window !== "undefined"
          ? localStorage.getItem("vigil_token")
          : null;

      if (!token) return; // not logged in — don't connect

      const url = `${BASE_WS}/api/ws?token=${encodeURIComponent(token)}`;

      const ws = new WebSocket(url);
      wsRef.current = ws;

      ws.onopen = () => {
        if (dead.current) {
          ws.close();
          return;
        }
        retryDelay.current = 1_000; // reset back-off on successful connect
        setConnected(true);
      };

      ws.onmessage = (evt) => {
        if (dead.current) return;
        try {
          const msg = JSON.parse(evt.data as string) as {
            type: string;
            count?: number;
            alerts?: AlertItem[];
          };
          if (msg.type === "new_alerts" && Array.isArray(msg.alerts)) {
            setLatestAlerts(msg.alerts);
            setUnreadCount((n) => n + (msg.count ?? msg.alerts!.length));
          }
          // "connected" and "ping" messages are silently ignored
        } catch {
          // malformed JSON — ignore
        }
      };

      ws.onerror = () => {
        // onerror always fires before onclose — just let onclose handle retry
        ws.close();
      };

      ws.onclose = () => {
        if (dead.current) return;
        setConnected(false);
        const delay = Math.min(retryDelay.current, 30_000);
        retryDelay.current = delay * 2;
        retryTimer.current = setTimeout(connect, delay);
      };
    }

    connect();

    return () => {
      dead.current = true;
      if (retryTimer.current != null) clearTimeout(retryTimer.current);
      if (wsRef.current) {
        // Null out onclose so the handler doesn't schedule another retry
        wsRef.current.onclose = null;
        wsRef.current.close();
        wsRef.current = null;
      }
      setConnected(false);
    };
  }, []); // mount/unmount only

  return { unreadCount, latestAlerts, clearCount, connected };
}
