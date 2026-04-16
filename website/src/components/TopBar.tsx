import { useState } from "react";
import { Plus, Minus, Wallet, TrendingUp } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Dialog, DialogContent, DialogHeader, DialogTitle, DialogTrigger } from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";

export interface NewAccountDetails {
  type: "master" | "slave";
  name: string;
  accountNumber: string;
  masterPass: string;
  server: string;
  mt5Path: string;
  xauSymbol: string;
}

interface TopBarProps {
  totalMasterBalance: number;
  totalSlaveBalance: number;
  onAddAccount: (details: NewAccountDetails) => void;
  onRemoveAccount: (type: "master" | "slave", id: string) => void;
  masterAccounts: {id: string;name: string;}[];
  slaveAccounts: {id: string;name: string;}[];
}

const emptyForm = { name: "", accountNumber: "", masterPass: "", server: "", mt5Path: "", xauSymbol: "XAUUSD" };

const TopBar = ({ totalMasterBalance, totalSlaveBalance, onAddAccount, onRemoveAccount, masterAccounts, slaveAccounts }: TopBarProps) => {
  const [newAccountType, setNewAccountType] = useState<"master" | "slave">("master");
  const [form, setForm] = useState(emptyForm);
  const [pathCheck, setPathCheck] = useState<{ status: "idle" | "checking" | "ok" | "missing" | "error"; message: string }>({
    status: "idle",
    message: "",
  });
  const [removeType, setRemoveType] = useState<"master" | "slave">("master");
  const [removeId, setRemoveId] = useState("");
  const [addOpen, setAddOpen] = useState(false);
  const [removeOpen, setRemoveOpen] = useState(false);

  const setField = (field: keyof typeof emptyForm) => (e: React.ChangeEvent<HTMLInputElement>) => {
    const value = e.target.value;
    setForm((prev) => ({ ...prev, [field]: value }));
    if (field === "mt5Path") setPathCheck({ status: "idle", message: "" });
  };

  const handleAdd = () => {
    const mt5Path = form.mt5Path.trim();
    const xauSymbol = form.xauSymbol.trim() || "XAUUSD";
    if (form.name.trim() && form.accountNumber.trim() && form.server.trim() && mt5Path) {
      onAddAccount({ type: newAccountType, ...form, mt5Path, xauSymbol });
      setForm(emptyForm);
      setPathCheck({ status: "idle", message: "" });
      setAddOpen(false);
    }
  };

  const checkPath = async () => {
    const mt5Path = form.mt5Path.trim();
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

  const handleRemove = () => {
    if (removeId) {
      onRemoveAccount(removeType, removeId);
      setRemoveId("");
      setRemoveOpen(false);
    }
  };

  const removeList = removeType === "master" ? masterAccounts : slaveAccounts;

  return (
    <header className="bg-topbar text-topbar-foreground px-3 py-3 sm:px-6 sm:py-4">
      <div className="max-w-[1600px] mx-auto flex items-center justify-between gap-2 sm:gap-6">
        {/* Brand */}
        <div className="flex items-center">
          <span className="text-base sm:text-lg font-semibold tracking-tight font-display">DPR System</span>
        </div>

        {/* Balances */}
        <div className="flex items-center gap-3 sm:gap-8">
          <div className="flex items-center gap-2 sm:gap-3">
            <div className="w-8 h-8 sm:w-10 sm:h-10 rounded-xl bg-master-muted flex items-center justify-center">
              <TrendingUp className="w-4 h-4 sm:w-5 sm:h-5 text-master-accent" />
            </div>
            <div>
              <p className="hidden sm:block text-xs text-topbar-foreground/50 uppercase tracking-wider font-medium">Master Balance</p>
              <div className="flex items-center gap-1.5 sm:gap-2">
                <span className="text-base sm:text-xl font-semibold font-mono">${totalMasterBalance.toLocaleString()}</span>
              </div>
            </div>
          </div>

          <div className="hidden sm:block w-px h-10 bg-topbar-foreground/10" />

          <div className="flex items-center gap-2 sm:gap-3">
            <div className="w-8 h-8 sm:w-10 sm:h-10 rounded-xl bg-master-muted flex items-center justify-center">
              <Wallet className="w-4 h-4 sm:w-5 sm:h-5 text-badge-info" />
            </div>
            <div>
              <p className="hidden sm:block text-xs text-topbar-foreground/50 uppercase tracking-wider font-medium">Slave Balance</p>
              <div className="flex items-center gap-1.5 sm:gap-2">
                <span className="text-base sm:text-xl font-semibold font-mono">${totalSlaveBalance.toLocaleString()}</span>
              </div>
            </div>
          </div>
        </div>

        {/* Actions */}
        <div className="flex items-center gap-1.5 sm:gap-2">
          <Dialog
            open={addOpen}
            onOpenChange={(open) => {
              setAddOpen(open);
              if (!open) {
                setForm(emptyForm);
                setPathCheck({ status: "idle", message: "" });
              }
            }}
          >
            <DialogTrigger asChild>
              <Button size="sm" className="gap-1.5">
                <Plus className="w-4 h-4" /> <span className="hidden sm:inline">Add Account</span>
              </Button>
            </DialogTrigger>
            <DialogContent>
              <DialogHeader>
                <DialogTitle>Add New Account</DialogTitle>
              </DialogHeader>
              <div className="space-y-4 pt-2">
                <div className="space-y-1.5">
                  <Label>Account Type</Label>
                  <Select value={newAccountType} onValueChange={(v) => setNewAccountType(v as "master" | "slave")}>
                    <SelectTrigger><SelectValue /></SelectTrigger>
                    <SelectContent>
                      <SelectItem value="master">Master Account</SelectItem>
                      <SelectItem value="slave">Slave Account</SelectItem>
                    </SelectContent>
                  </Select>
                </div>
                <div className="space-y-1.5">
                  <Label>Account Name</Label>
                  <Input placeholder="e.g. Client A" value={form.name} onChange={setField("name")} />
                </div>
                <div className="space-y-1.5">
                  <Label>Account Number</Label>
                  <Input placeholder="e.g. 123456" value={form.accountNumber} onChange={setField("accountNumber")} />
                </div>
                <div className="space-y-1.5">
                  <Label>Master Password</Label>
                  <Input type="password" placeholder="Master password" value={form.masterPass} onChange={setField("masterPass")} />
                </div>
                <div className="space-y-1.5">
                  <Label>Server</Label>
                  <Input placeholder="e.g. ICMarkets-Live01" value={form.server} onChange={setField("server")} onKeyDown={(e) => e.key === "Enter" && handleAdd()} />
                </div>
                <div className="space-y-1.5">
                  <Label>MT5 Terminal Path (Required)</Label>
                  <Input
                    placeholder="e.g. C:\\Program Files\\MetaTrader 5\\terminal64.exe"
                    value={form.mt5Path}
                    onChange={setField("mt5Path")}
                  />
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
                  <Input
                    placeholder="e.g. XAUUSD, XAUUSD!, Gold!"
                    value={form.xauSymbol}
                    onChange={setField("xauSymbol")}
                  />
                  <p className="text-[11px] text-muted-foreground">Exact symbol name as it appears in MT5 for this account.</p>
                </div>
                <Button
                  onClick={handleAdd}
                  className="w-full"
                  disabled={!form.name.trim() || !form.accountNumber.trim() || !form.server.trim() || !form.mt5Path.trim()}
                >
                  Create Account
                </Button>
              </div>
            </DialogContent>
          </Dialog>

          <Dialog open={removeOpen} onOpenChange={setRemoveOpen}>
            <DialogTrigger asChild>
              <Button size="sm" className="gap-1.5">
                <Minus className="w-4 h-4" /> <span className="hidden sm:inline">Remove</span>
              </Button>
            </DialogTrigger>
            <DialogContent>
              <DialogHeader>
                <DialogTitle>Remove Account</DialogTitle>
              </DialogHeader>
              <div className="space-y-4 pt-2">
                <Select value={removeType} onValueChange={(v) => {setRemoveType(v as "master" | "slave");setRemoveId("");}}>
                  <SelectTrigger><SelectValue /></SelectTrigger>
                  <SelectContent>
                    <SelectItem value="master">Master Account</SelectItem>
                    <SelectItem value="slave">Slave Account</SelectItem>
                  </SelectContent>
                </Select>
                <Select value={removeId} onValueChange={setRemoveId}>
                  <SelectTrigger><SelectValue placeholder="Select account" /></SelectTrigger>
                  <SelectContent>
                    {removeList.map((a) =>
                    <SelectItem key={a.id} value={a.id}>{a.name}</SelectItem>
                    )}
                  </SelectContent>
                </Select>
                <Button variant="destructive" onClick={handleRemove} className="w-full">Remove Account</Button>
              </div>
            </DialogContent>
          </Dialog>
        </div>
      </div>
    </header>);
};

export default TopBar;
