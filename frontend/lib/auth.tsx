"use client";
import { createContext, useContext, useEffect, useState, ReactNode } from "react";
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

  const refresh = async () => {
    try {
      const me = await getMe();
      setUser(me);
    } catch {
      setUser(null);
    }
  };

  useEffect(() => {
    const token = localStorage.getItem("vigil_token");
    if (!token) { setLoading(false); return; }
    refresh().finally(() => setLoading(false));
  }, []);

  return (
    <Ctx.Provider value={{ user, loading, logout: apiLogout, refresh }}>
      {children}
    </Ctx.Provider>
  );
}

export const useAuth = () => useContext(Ctx);
