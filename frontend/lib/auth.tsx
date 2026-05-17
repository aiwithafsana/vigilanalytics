"use client";
import { createContext, useCallback, useContext, useEffect, useState, ReactNode } from "react";
import { getMe, logout as apiLogout } from "@/lib/api";
import type { User } from "@/types";

interface AuthContext {
  user: User | null;
  loading: boolean;
  logout: () => void;
  refresh: () => Promise<void>;
}

const Ctx = createContext<AuthContext>({
  user: null,
  loading: true,
  logout: () => {},
  refresh: async () => {},
});

export function AuthProvider({ children }: { children: ReactNode }) {
  const [user, setUser] = useState<User | null>(null);
  const [loading, setLoading] = useState(true);

  // Memoised so its identity stays stable across renders.  Without this,
  // the [refresh] dep on the effect below would cause an infinite render
  // loop — every render creates a new function, deps change, effect re-runs,
  // effect calls setUser → re-render → new function → ...
  // setUser from useState is itself stable, so an empty dep array is correct.
  const refresh = useCallback(async () => {
    try {
      const me = await getMe();
      setUser(me);
    } catch {
      setUser(null);
    }
  }, []);

  useEffect(() => {
    const token = localStorage.getItem("vigil_token");
    // No token = nothing to refresh; flip loading=false synchronously so the
    // login redirect can fire on the very next render.  React 19's set-state-
    // in-effect rule warns here, but a deferred update would briefly render
    // a "loading" UI for an unauthenticated user, then immediately redirect.
    // eslint-disable-next-line react-hooks/set-state-in-effect
    if (!token) { setLoading(false); return; }
    refresh().finally(() => setLoading(false));
  }, [refresh]);

  return (
    <Ctx.Provider value={{ user, loading, logout: apiLogout, refresh }}>
      {children}
    </Ctx.Provider>
  );
}

export const useAuth = () => useContext(Ctx);
