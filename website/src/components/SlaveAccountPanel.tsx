import { useState } from "react";
import { Link2, ArrowUpRight, ArrowDownRight, Users, Pencil, Wifi, WifiOff, AlertTriangle, Loader } from "lucide-react";
import { Dialog, DialogContent, DialogHeader, DialogTitle } from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Button } from "@/components/ui/button";

export interface SlaveAccount {
  id: string;
  name: string;
  accountNumber: string;
  masterPass: string;
  broker: string;
  mt5Path?: string;
  currency: string;
  masterId: string;
  balance: number;
  equity: number;
  pnl: number;
  totalPnl: number;
  mtStatus?: "connected" | "disconnected" | "error" | "connecting";
  mtMessage?: string;
  createdAt: string;
  lastUpdated: string;
}

type MasterMode = "manual" | "automated";

interface SlaveAccountPanelProps {
  accounts: SlaveAccount[];
  masterId: string | null;
  masterName: string | null;
  masterMode?: MasterMode;
  onMasterModeChange?: (masterId: string, mode: MasterMode) => void;
  onEdit: (id: string, updates: Partial<SlaveAccount>) => void;
}

function SlaveMt5Badge({ status, message }: { status?: string; message?: string }) {
  if (!status || status === "disconnected") {
    return (
      <span className="flex items-center gap-1 text-[10px] text-muted-foreground">
        <WifiOff className="w-3 h-3" />
        <span>MT5 offline</span>
      </span>
    );
  }
  if (status === "connecting") {
    return (
      <span className="flex items-center gap-1 text-[10px] text-badge-warning">
        <Loader className="w-3 h-3 animate-spin" />
        <span>Connecting...</span>
      </span>
    );
  }
  if (status === "error") {
    return (
      <span className="flex items-center gap-1 text-[10px] text-destructive" title={message || "MT5 error"}>
        <AlertTriangle className="w-3 h-3" />
        <span className="max-w-[120px] truncate">{message || "MT5 error"}</span>
      </span>
    );
  }
  return (
    <span className="flex items-center gap-1 text-[10px] text-badge-success">
      <Wifi className="w-3 h-3" />
      <span>MT5 live</span>
    </span>
  );
}

const SlaveAccountPanel = ({
  accounts,
  masterId,
  masterName,
  masterMode,
  onMasterModeChange,
  onEdit,
}: SlaveAccountPanelProps) => {
  const [editAccount, setEditAccount] = useState<SlaveAccount | null>(null);
  const [form, setForm] = useState<Partial<SlaveAccount>>({});
  const [pathCheck, setPathCheck] = useState<{ status: "idle" | "checking" | "ok" | "missing" | "error"; message: string }>({
    status: "idle",
    message: "",
  });
  const resolvedMasterMode: MasterMode = masterMode === "automated" ? "automated" : "manual";

  const updateMode = (mode: MasterMode) => {
    if (!masterId || !onMasterModeChange || resolvedMasterMode === mode) return;
    onMasterModeChange(masterId, mode);
  };

  const openEdit = (account: SlaveAccount) => {
    setEditAccount(account);
    setForm({
      name: account.name,
      accountNumber: account.accountNumber,
      masterPass: account.masterPass,
      broker: account.broker,
      mt5Path: account.mt5Path ?? "",
    });
    setPathCheck({ status: "idle", message: "" });
  };

  const handleSave = () => {
    if (editAccount) {
      const mt5Path = String(form.mt5Path || "").trim();
      if (!mt5Path) return;
      onEdit(editAccount.id, { ...form, mt5Path });
      setEditAccount(null);
      setPathCheck({ status: "idle", message: "" });
    }
  };

  const setField = (field: keyof SlaveAccount) => (e: React.ChangeEvent<HTMLInputElement>) => {
    setForm((prev) => ({ ...prev, [field]: e.target.value }));
    if (field === "mt5Path") setPathCheck({ status: "idle", message: "" });
  };

  const checkPath = async () => {
    const mt5Path = String(form.mt5Path || "").trim();
    if (!mt5Path) {
      setPathCheck({ status: "error", message: "Enter MT5 terminal path first." });
      return;
    }

    setPathCheck({ status: "checking", message: "Checking path on engine host..." });
    try {
      const res = await fetch("/api/mt5-path/check", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ mt5Path }),
      });
      const body = await res.json().catch(() => ({}));
      if (!res.ok) throw new Error(body?.error || "Failed to check path");

      if (body.exists) {
        setPathCheck({ status: "ok", message: "Path found on engine host." });
      } else {
        setPathCheck({ status: "missing", message: "Path not found on engine host." });
      }
    } catch (e) {
      setPathCheck({ status: "error", message: e instanceof Error ? e.message : "Failed to check MT5 path." });
    }
  };

  if (!masterName) {
    return (
      <div className="h-full flex items-center justify-center bg-slave text-slave-foreground">
        <div className="text-center">
          <Users className="w-12 h-12 sm:w-16 sm:h-16 mx-auto mb-4 text-muted-foreground/30" />
          <p className="text-base sm:text-lg font-medium text-muted-foreground/60">Select a Master Account</p>
          <p className="text-xs sm:text-sm text-muted-foreground/40 mt-1">Click on a master account to view linked slaves</p>
        </div>
      </div>
    );
  }

  return (
    <div className="h-full bg-slave text-slave-foreground flex flex-col">
      <div className="p-3 sm:p-5 border-b border-border">
        <div className="flex items-center gap-2 mb-1">
          <Link2 className="w-4 h-4 sm:w-5 sm:h-5 text-primary" />
          <h2 className="text-sm sm:text-base font-semibold tracking-tight">Slave Accounts</h2>
        </div>
        <p className="text-xs text-muted-foreground">
          Linked to <span className="text-primary font-medium">{masterName}</span> · {accounts.length} accounts
        </p>
        <div className="mt-3 flex flex-wrap items-center justify-between gap-2 rounded-lg border border-border bg-card/60 px-3 py-2">
          <span className="text-[11px] font-semibold uppercase tracking-wider text-muted-foreground">Execution Mode</span>
          <div className="inline-flex items-center rounded-md border border-border bg-background p-0.5">
            <Button
              type="button"
              size="sm"
              variant={resolvedMasterMode === "manual" ? "default" : "ghost"}
              className="h-7 px-2 text-[11px]"
              onClick={() => updateMode("manual")}
            >
              Manual
            </Button>
            <Button
              type="button"
              size="sm"
              variant={resolvedMasterMode === "automated" ? "default" : "ghost"}
              className="h-7 px-2 text-[11px]"
              onClick={() => updateMode("automated")}
            >
              Automated
            </Button>
          </div>
        </div>
        <p className="mt-1 text-[11px] text-muted-foreground">
          {resolvedMasterMode === "automated"
            ? "Automated mode selected. Automation behavior will be connected in a later update."
            : "Manual mode is active by default."}
        </p>
      </div>

      <div className="flex-1 overflow-y-auto p-3 sm:p-4">
        <div className="grid gap-2 sm:gap-3 grid-cols-1 lg:grid-cols-2">
          {accounts.map((account) => {
            return (
              <div
                key={account.id}
                className="rounded-xl border border-border p-3 sm:p-4 hover:shadow-md transition-shadow duration-200 bg-card group"
              >
                {/* Header row */}
                <div className="flex items-center justify-between mb-2">
                  <div className="flex items-center gap-2">
                    <div className="w-7 h-7 sm:w-8 sm:h-8 rounded-lg bg-secondary flex items-center justify-center">
                      <span className="text-xs font-bold text-secondary-foreground">
                        {account.name.charAt(0)}
                      </span>
                    </div>
                    <div>
                      <p className="text-xs sm:text-sm font-semibold">{account.name}</p>
                      <p className="text-[10px] text-muted-foreground">{account.broker} · {account.accountNumber}</p>
                    </div>
                  </div>
                  <button
                    onClick={() => openEdit(account)}
                    className="opacity-0 group-hover:opacity-100 transition-opacity p-1 rounded-md hover:bg-muted"
                    title="Edit account"
                  >
                    <Pencil className="w-3 h-3 text-muted-foreground" />
                  </button>
                </div>

                {/* Stats grid */}
                <div className="grid grid-cols-2 gap-2 mt-3">
                  <div>
                    <p className="text-[10px] text-muted-foreground mb-0.5">Balance</p>
                    <p className="text-base sm:text-lg font-semibold font-mono">${account.balance.toLocaleString()}</p>
                  </div>
                  <div>
                    <p className="text-[10px] text-muted-foreground mb-0.5">Equity</p>
                    <p className="text-base sm:text-lg font-semibold font-mono">${account.equity.toLocaleString()}</p>
                  </div>
                  <div>
                    <p className="text-[10px] text-muted-foreground mb-0.5">Today P&L</p>
                    <p className={`text-sm font-semibold font-mono flex items-center gap-0.5 ${account.pnl >= 0 ? "text-badge-success" : "text-destructive"}`}>
                      {account.pnl >= 0 ? <ArrowUpRight className="w-3.5 h-3.5" /> : <ArrowDownRight className="w-3.5 h-3.5" />}
                      ${Math.abs(account.pnl).toLocaleString()}
                    </p>
                  </div>
                  <div>
                    <p className="text-[10px] text-muted-foreground mb-0.5">Total P&L</p>
                    <p className={`text-sm font-semibold font-mono flex items-center gap-0.5 ${account.totalPnl >= 0 ? "text-badge-success" : "text-destructive"}`}>
                      {account.totalPnl >= 0 ? <ArrowUpRight className="w-3.5 h-3.5" /> : <ArrowDownRight className="w-3.5 h-3.5" />}
                      ${Math.abs(account.totalPnl).toLocaleString()}
                    </p>
                  </div>
                </div>

                <div className="mt-2">
                  <SlaveMt5Badge status={account.mtStatus} message={account.mtMessage} />
                </div>
                {account.mt5Path && (
                  <div className="mt-2 text-[10px] text-muted-foreground truncate" title={account.mt5Path}>
                    MT5: {account.mt5Path}
                  </div>
                )}
              </div>
            );
          })}
        </div>
      </div>

      {/* Edit Dialog */}
      <Dialog
        open={!!editAccount}
        onOpenChange={(open) => {
          if (!open) {
            setEditAccount(null);
            setPathCheck({ status: "idle", message: "" });
          }
        }}
      >
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Edit Slave Account</DialogTitle>
          </DialogHeader>
          <div className="space-y-4 pt-2">
            <div className="space-y-1.5">
              <Label>Account Name</Label>
              <Input value={form.name ?? ""} onChange={setField("name")} />
            </div>
            <div className="space-y-1.5">
              <Label>Account Number</Label>
              <Input value={form.accountNumber ?? ""} onChange={setField("accountNumber")} />
            </div>
            <div className="space-y-1.5">
              <Label>Master Password</Label>
              <Input type="password" value={form.masterPass ?? ""} onChange={setField("masterPass")} />
            </div>
            <div className="space-y-1.5">
              <Label>Server</Label>
              <Input value={form.broker ?? ""} onChange={setField("broker")} />
            </div>
            <div className="space-y-1.5">
              <Label>MT5 Terminal Path (Required)</Label>
              <Input value={form.mt5Path ?? ""} onChange={setField("mt5Path")} />
              <div className="flex items-center justify-between gap-2">
                <p
                  className={`text-[11px] ${
                    pathCheck.status === "ok"
                      ? "text-badge-success"
                      : pathCheck.status === "missing" || pathCheck.status === "error"
                        ? "text-destructive"
                        : "text-muted-foreground"
                  }`}
                >
                  {pathCheck.message || "Each account must have a unique MT5 path."}
                </p>
                <Button type="button" variant="outline" size="sm" onClick={checkPath} disabled={pathCheck.status === "checking"}>
                  {pathCheck.status === "checking" ? "Checking..." : "Check Path"}
                </Button>
              </div>
            </div>
            <Button onClick={handleSave} className="w-full" disabled={!String(form.mt5Path || "").trim()}>Save Changes</Button>
          </div>
        </DialogContent>
      </Dialog>
    </div>
  );
};

export default SlaveAccountPanel;
