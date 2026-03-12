import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { useState, useRef } from "react";
import TopBar, { NewAccountDetails } from "@/components/TopBar";
import MasterAccountList, { MasterAccount } from "@/components/MasterAccountList";
import SlaveAccountPanel, { SlaveAccount } from "@/components/SlaveAccountPanel";
import { SystemStatusPanel } from "@/components/SystemStatusPanel";
import { toast } from "@/components/ui/sonner";

interface PathWarningAccountRef {
  kind: "master" | "slave";
  id: string;
  name?: string;
  mt5Path?: string;
}

interface Mt5PathWarnings {
  missingPaths: PathWarningAccountRef[];
  duplicatePaths: { mt5Path: string; accounts: PathWarningAccountRef[] }[];
  unavailablePaths: PathWarningAccountRef[];
}

interface DashboardData {
  masterAccounts: MasterAccount[];
  slaveAccounts: SlaveAccount[];
  mt5PathWarnings?: Mt5PathWarnings;
}

async function requestJson(url: string, init?: RequestInit) {
  const res = await fetch(url, init);
  const text = await res.text();
  let body: any = null;
  if (text) {
    try { body = JSON.parse(text); } catch (_e) { body = null; }
  }
  if (!res.ok) {
    throw new Error(body?.error || body?.message || `Request failed (${res.status})`);
  }
  return body;
}

async function fetchData(): Promise<DashboardData> {
  return requestJson("/api/data");
}

const Index = () => {
  const queryClient = useQueryClient();
  const [selectedMasterId, setSelectedMasterId] = useState<string | null>(null);

  const { data, isLoading, isError, error } = useQuery({
    queryKey: ["data"],
    queryFn: fetchData,
    refetchInterval: 5000, // auto-refresh every 5 seconds
  });

  const mastersRaw: MasterAccount[] = data?.masterAccounts ?? [];
  const masters: MasterAccount[] = mastersRaw.map((master) => ({
    ...master,
    mode: master.mode === "automated" ? "automated" : "manual",
  }));
  const slaves: SlaveAccount[] = data?.slaveAccounts ?? [];
  const mt5PathWarnings: Mt5PathWarnings = data?.mt5PathWarnings ?? {
    missingPaths: [],
    duplicatePaths: [],
    unavailablePaths: [],
  };

  const maxId = [...masters, ...slaves].reduce((max, a) => {
    const n = parseInt(a.id.slice(1));
    return isNaN(n) ? max : Math.max(max, n);
  }, 99);
  const nextIdRef = useRef(maxId + 1);
  if (maxId >= nextIdRef.current) nextIdRef.current = maxId + 1;

  // Set default selection once data loads
  if (selectedMasterId === null && masters.length > 0) {
    setSelectedMasterId(masters[0].id);
  }

  const addMutation = useMutation({
    mutationFn: async ({ type, account }: { type: "master" | "slave"; account: MasterAccount | SlaveAccount }) => {
      const url = type === "master" ? "/api/masters" : "/api/slaves";
      await requestJson(url, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(account),
      });
    },
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ["data"] }),
    onError: (e: unknown) => toast.error(e instanceof Error ? e.message : "Failed to add account"),
  });

  const removeMutation = useMutation({
    mutationFn: async ({ type, id }: { type: "master" | "slave"; id: string }) => {
      const url = type === "master" ? `/api/masters/${id}` : `/api/slaves/${id}`;
      await requestJson(url, { method: "DELETE" });
    },
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ["data"] }),
    onError: (e: unknown) => toast.error(e instanceof Error ? e.message : "Failed to remove account"),
  });

  const handleAddAccount = ({ type, name, accountNumber, masterPass, server, mt5Path }: NewAccountDetails) => {
    const id = `${type[0]}${nextIdRef.current++}`;
    const today = new Date().toISOString().split("T")[0];
    const normalizedMt5Path = mt5Path.trim();
    if (type === "master") {
      const newMaster: MasterAccount = {
        id, name,
        accountNumber,
        masterPass,
        broker: server,
        mt5Path: normalizedMt5Path,
        mode: "manual",
        currency: "USD",
        balance: 0, equity: 0, margin: 0, freeMargin: 0,
        pnl: 0, totalPnl: 0,
        status: "paused", slaveCount: 0,
        createdAt: today, lastUpdated: today,
      };
      addMutation.mutate({ type, account: newMaster });
    } else {
      const masterId = selectedMasterId || masters[0]?.id || "";
      const newSlave: SlaveAccount = {
        id, name,
        accountNumber,
        masterPass,
        broker: server,
        mt5Path: normalizedMt5Path,
        currency: "USD",
        masterId,
        balance: 0, equity: 0,
        pnl: 0, totalPnl: 0,
        copyRatio: 0.1,
        status: "pending",
        mtStatus: "disconnected",
        mtMessage: "Waiting for worker connection",
        createdAt: today, lastUpdated: today,
      };
      addMutation.mutate({ type, account: newSlave });
    }
  };

  const activateMutation = useMutation({
    mutationFn: async (id: string) => {
      await requestJson(`/api/masters/${id}/activate`, { method: "POST" });
    },
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ["data"] }),
    onError: (e: unknown) => toast.error(e instanceof Error ? e.message : "Failed to activate master"),
  });

  const deactivateMutation = useMutation({
    mutationFn: async (id: string) => {
      await requestJson(`/api/masters/${id}`, {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ status: "paused" }),
      });
    },
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ["data"] }),
    onError: (e: unknown) => toast.error(e instanceof Error ? e.message : "Failed to deactivate master"),
  });

  const editMutation = useMutation({
    mutationFn: async ({ type, id, updates }: { type: "master" | "slave"; id: string; updates: object }) => {
      const url = type === "master" ? `/api/masters/${id}` : `/api/slaves/${id}`;
      await requestJson(url, {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(updates),
      });
    },
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ["data"] }),
    onError: (e: unknown) => toast.error(e instanceof Error ? e.message : "Failed to update account"),
  });

  const handleRemoveAccount = (type: "master" | "slave", id: string) => {
    if (type === "master" && selectedMasterId === id) setSelectedMasterId(null);
    removeMutation.mutate({ type, id });
  };

  const handleEditMaster = (id: string, updates: Partial<MasterAccount>) => {
    const normalized = { ...updates };
    if (typeof normalized.mt5Path === "string") normalized.mt5Path = normalized.mt5Path.trim();
    if (Object.prototype.hasOwnProperty.call(normalized, "mode")) {
      normalized.mode = normalized.mode === "automated" ? "automated" : "manual";
    }
    editMutation.mutate({ type: "master", id, updates: normalized });
  };

  const handleMasterModeChange = (id: string, mode: "manual" | "automated") => {
    handleEditMaster(id, { mode });
  };

  const handleEditSlave = (id: string, updates: Partial<SlaveAccount>) => {
    const normalized = { ...updates };
    if (typeof normalized.mt5Path === "string") normalized.mt5Path = normalized.mt5Path.trim();
    editMutation.mutate({ type: "slave", id, updates: normalized });
  };

  const filteredSlaves = selectedMasterId ? slaves.filter((s) => s.masterId === selectedMasterId) : [];
  const selectedMaster = masters.find((m) => m.id === selectedMasterId);
  const totalMasterBalance = masters.reduce((s, a) => s + a.balance, 0);
  const totalSlaveBalance = slaves.reduce((s, a) => s + a.balance, 0);
  const hasPathWarnings =
    mt5PathWarnings.missingPaths.length > 0 ||
    mt5PathWarnings.duplicatePaths.length > 0 ||
    mt5PathWarnings.unavailablePaths.length > 0;
  const accountRefLabel = (a: PathWarningAccountRef) => `${a.kind}:${a.id}${a.name ? ` (${a.name})` : ""}`;

  if (isLoading) {
    return (
      <div className="h-screen flex items-center justify-center bg-background text-muted-foreground">
        Loading...
      </div>
    );
  }

  if (isError) {
    return (
      <div className="h-screen flex items-center justify-center bg-background text-destructive">
        Failed to connect to API: {error instanceof Error ? error.message : "Make sure the server is running."}
      </div>
    );
  }

  return (
    <div className="h-screen flex flex-col overflow-hidden">
      <TopBar
        totalMasterBalance={totalMasterBalance}
        totalSlaveBalance={totalSlaveBalance}
        onAddAccount={handleAddAccount}
        onRemoveAccount={handleRemoveAccount}
        masterAccounts={masters}
        slaveAccounts={slaves}
      />
      <SystemStatusPanel />
      {hasPathWarnings && (
        <div className="px-3 sm:px-6 pt-3 space-y-2">
          <div className="max-w-[1600px] mx-auto space-y-2">
            {mt5PathWarnings.missingPaths.length > 0 && (
              <div className="rounded-md border border-destructive/40 bg-destructive/10 px-3 py-2 text-[12px] text-destructive">
                Missing MT5 path: {mt5PathWarnings.missingPaths.map(accountRefLabel).join(", ")}
              </div>
            )}
            {mt5PathWarnings.duplicatePaths.map((dup) => (
              <div key={`dup-${dup.mt5Path}`} className="rounded-md border border-destructive/40 bg-destructive/10 px-3 py-2 text-[12px] text-destructive">
                Duplicate MT5 path "{dup.mt5Path}": {dup.accounts.map(accountRefLabel).join(", ")}
              </div>
            ))}
            {mt5PathWarnings.unavailablePaths.length > 0 && (
              <div className="rounded-md border border-amber-500/40 bg-amber-500/10 px-3 py-2 text-[12px] text-amber-700 dark:text-amber-300">
                MT5 path not found on engine host: {mt5PathWarnings.unavailablePaths.map((a) => `${accountRefLabel(a)} => ${a.mt5Path || "(empty)"}`).join(", ")}
              </div>
            )}
          </div>
        </div>
      )}

      <div className="flex-1 flex flex-col sm:flex-row overflow-hidden">
        <div className="w-full h-[40vh] sm:h-full sm:w-[240px] lg:w-[340px] flex-shrink-0 border-b sm:border-b-0 sm:border-r border-border">
          <MasterAccountList
            accounts={masters}
            selectedId={selectedMasterId}
            onSelect={setSelectedMasterId}
            onEdit={handleEditMaster}
            onActivate={(id) => activateMutation.mutate(id)}
            onDeactivate={(id) => deactivateMutation.mutate(id)}
          />
        </div>

        <div className="flex-1 min-h-0">
          <SlaveAccountPanel
            accounts={filteredSlaves}
            masterId={selectedMaster?.id || null}
            masterName={selectedMaster?.name || null}
            masterMode={selectedMaster?.mode}
            onMasterModeChange={handleMasterModeChange}
            onEdit={handleEditSlave}
          />
        </div>
      </div>
    </div>
  );
};

export default Index;
