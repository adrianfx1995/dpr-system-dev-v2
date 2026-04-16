import { useState } from "react";
import { Crown, Pencil, Zap, ZapOff, Wifi, WifiOff, AlertTriangle, Loader } from "lucide-react";
import { Dialog, DialogContent, DialogHeader, DialogTitle } from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Button } from "@/components/ui/button";

export interface MasterAccount {
  id: string;
  name: string;
  accountNumber: string;
  masterPass: string;
  broker: string;
  mt5Path?: string;
  mode?: "manual" | "automated";
  currency: string;
  balance: number;
  equity: number;
  margin: number;
  freeMargin: number;
  pnl: number;
  totalPnl: number;
  status: "active" | "paused";
  mtStatus?: "connected" | "disconnected" | "error" | "connecting";
  mtMessage?: string;
  xauSymbol?: string;
  slaveCount: number;
  createdAt: string;
  lastUpdated: string;
}

function MT5StatusBadge({ status, message }: { status?: string; message?: string }) {
  if (!status || status === "disconnected") {
    return (
      <span className="flex items-center gap-1 text-[10px] text-master-foreground/30">
        <WifiOff className="w-3 h-3" />
        <span>MT5 offline</span>
      </span>
    );
  }
  if (status === "connecting") {
    return (
      <span className="flex items-center gap-1 text-[10px] text-yellow-400">
        <Loader className="w-3 h-3 animate-spin" />
        <span>Connecting…</span>
      </span>
    );
  }
  if (status === "error") {
    return (
      <span className="flex items-center gap-1 text-[10px] text-destructive" title={message}>
        <AlertTriangle className="w-3 h-3" />
        <span className="max-w-[100px] truncate">{message || "MT5 error"}</span>
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

interface MasterAccountListProps {
  accounts: MasterAccount[];
  selectedId: string | null;
  onSelect: (id: string) => void;
  onEdit: (id: string, updates: Partial<MasterAccount>) => void;
  onActivate: (id: string) => void;
  onDeactivate: (id: string) => void;
}

const MasterAccountList = ({ accounts, selectedId, onSelect, onEdit, onActivate, onDeactivate }: MasterAccountListProps) => {
  const [editAccount, setEditAccount] = useState<MasterAccount | null>(null);
  const [form, setForm] = useState<Partial<MasterAccount>>({});
  const [pathCheck, setPathCheck] = useState<{ status: "idle" | "checking" | "ok" | "missing" | "error"; message: string }>({
    status: "idle",
    message: "",
  });

  const openEdit = (e: React.MouseEvent, account: MasterAccount) => {
    e.stopPropagation();
    setEditAccount(account);
    setForm({
      name: account.name,
      accountNumber: account.accountNumber,
      masterPass: account.masterPass,
      broker: account.broker,
      mt5Path: account.mt5Path ?? "",
      xauSymbol: account.xauSymbol ?? "XAUUSD",
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

  const setField = (field: keyof MasterAccount) => (e: React.ChangeEvent<HTMLInputElement>) => {
    const value = e.target.value;
    setForm((prev) => ({ ...prev, [field]: value }));
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

  return (
    <div className="h-full bg-master text-master-foreground flex flex-col">
      <div className="p-3 sm:p-5 border-b border-master-muted/50">
        <div className="flex items-center gap-2 mb-1">
          <Crown className="w-4 h-4 sm:w-5 sm:h-5 text-master-accent" />
          <h2 className="text-sm sm:text-base font-semibold tracking-tight">Master Accounts</h2>
        </div>
        <p className="text-xs text-master-foreground/40">{accounts.length} accounts configured</p>
      </div>

      <div className="flex-1 overflow-y-auto p-2 sm:p-3 space-y-1 sm:space-y-1.5">
        {accounts.map((account) => {
          const isActive = account.status === "active";
          return (
            <div
              key={account.id}
              onClick={() => onSelect(account.id)}
              className={`w-full text-left rounded-xl p-3 sm:p-4 transition-all duration-200 group cursor-pointer ${
                isActive ? "opacity-100" : "opacity-55"
              } ${
                selectedId === account.id
                  ? "bg-master-accent/15 border border-master-accent/30"
                  : "hover:bg-master-muted/60 border border-transparent"
              } ${isActive ? "ring-1 ring-badge-success/40" : ""}`}
            >
              {/* Header row */}
              <div className="flex items-center justify-between mb-1.5">
                <div className="flex items-center gap-2">
                  <span
                    className={`inline-block w-2.5 h-2.5 rounded-full flex-shrink-0 ${
                      isActive ? "bg-badge-success shadow-[0_0_0_3px_rgba(34,197,94,0.2)]" : "bg-master-foreground/25"
                    }`}
                    aria-label={isActive ? "Active account" : "Paused account"}
                  />
                  <span className="font-medium text-xs sm:text-sm">{account.name}</span>
                </div>
                <button
                  onClick={(e) => openEdit(e, account)}
                  className="opacity-0 group-hover:opacity-100 transition-opacity p-1 rounded-md hover:bg-master-muted/80"
                  title="Edit account"
                >
                  <Pencil className="w-3 h-3 text-master-foreground/60" />
                </button>
              </div>

              <div className="flex items-center justify-between mb-2 pl-4">
                <p className="text-[10px] text-master-foreground/40">{account.broker} · {account.accountNumber}</p>
                <MT5StatusBadge status={account.mtStatus} message={account.mtMessage} />
              </div>
              {account.mt5Path && (
                <div className="mb-2 pl-4 text-[10px] text-master-foreground/50 truncate" title={account.mt5Path}>
                  MT5: {account.mt5Path}
                </div>
              )}

              <div className="flex items-end justify-between">
                <div>
                  <p className="text-[10px] text-master-foreground/40 mb-0.5">Balance</p>
                  <span className="text-base sm:text-lg font-semibold font-mono text-master-foreground/90">
                    ${account.balance.toLocaleString()}
                  </span>
                </div>
                <div className="text-right">
                  <p className="text-[10px] text-master-foreground/40 mb-0.5">Today P&L</p>
                  <span className={`text-xs font-semibold font-mono ${account.pnl >= 0 ? "text-badge-success" : "text-destructive"}`}>
                    {account.pnl >= 0 ? "+" : ""}${account.pnl.toLocaleString()}
                  </span>
                </div>
              </div>

              <div className="flex items-center justify-between mt-2 pt-2 border-t border-master-muted/30">
                <span className="text-[10px] text-master-foreground/40">
                  Equity: <span className="text-master-foreground/70">${account.equity.toLocaleString()}</span>
                  <span className="ml-2">· {account.slaveCount} slaves</span>
                  <span className="ml-2">· {(account.mode === "automated" ? "Automated" : "Manual")} mode</span>
                </span>
                {/* Always-visible Activate / Deactivate button */}
                {isActive ? (
                  <button
                    onClick={(e) => { e.stopPropagation(); onDeactivate(account.id); }}
                    className="flex items-center gap-1 px-2 py-1 rounded-md bg-destructive/10 hover:bg-destructive/20 text-destructive transition-colors"
                    title="Deactivate"
                  >
                    <ZapOff className="w-3 h-3" />
                    <span className="text-[10px] font-semibold">Deactivate</span>
                  </button>
                ) : (
                  <button
                    onClick={(e) => { e.stopPropagation(); onActivate(account.id); }}
                    className="flex items-center gap-1 px-2 py-1 rounded-md bg-badge-success/15 hover:bg-badge-success/25 text-badge-success transition-colors"
                    title="Activate"
                  >
                    <Zap className="w-3 h-3" />
                    <span className="text-[10px] font-semibold">Activate</span>
                  </button>
                )}
              </div>
            </div>
          );
        })}
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
            <DialogTitle>Edit Master Account</DialogTitle>
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
            <div className="space-y-1.5">
              <Label>XAUUSD Symbol Name (on this broker)</Label>
              <Input placeholder="e.g. XAUUSD, XAUUSD!, Gold!" value={form.xauSymbol ?? ""} onChange={setField("xauSymbol")} />
              <p className="text-[11px] text-muted-foreground">Exact symbol name as it appears in MT5 for this account.</p>
            </div>
            <Button onClick={handleSave} className="w-full" disabled={!String(form.mt5Path || "").trim()}>Save Changes</Button>
          </div>
        </DialogContent>
      </Dialog>
    </div>
  );
};

export default MasterAccountList;
