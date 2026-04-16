# -*- coding: utf-8 -*-
"""
Master EA manager.
Spawns one master_ea_worker.py process per active master account.
"""

import os
import sys
import time
import json
import signal
import subprocess
import urllib.request

API_BASE = os.environ.get("DPR_API_BASE", "http://localhost:3001")
ENGINE_HOST = os.environ.get("DPR_ENGINE_HOST", "127.0.0.1")
ENGINE_PORT = int(os.environ.get("DPR_ENGINE_PORT", "9090"))
POLL_SECONDS = int(os.environ.get("DPR_MASTER_POLL_SECONDS", "10"))

RUNNING = True


def fetch_db():
    url = f"{API_BASE}/api/data"
    with urllib.request.urlopen(urllib.request.Request(url), timeout=8) as resp:
        return json.loads(resp.read().decode())


def normalize_master(master):
    master_id = master.get("id")
    account = str(master.get("accountNumber") or "").strip()
    password = str(master.get("masterPass") or "")
    server = str(master.get("broker") or "").strip()
    status = str(master.get("status") or "").strip().lower()

    if status != "active":
        return None
    if not master_id or not account or not password or not server:
        return None

    mt5_path = str(master.get("mt5Path") or "").strip()
    if not mt5_path:
        return None
    xau_symbol = str(master.get("xauSymbol") or "XAUUSD").strip() or "XAUUSD"
    return {
        "master_id": str(master_id),
        "account": account,
        "password": password,
        "server": server,
        "mt5_path": mt5_path,
        "xau_symbol": xau_symbol,
    }


def build_command(cfg):
    worker = os.path.join(os.path.dirname(os.path.abspath(__file__)), "master_ea_worker.py")
    return [
        sys.executable,
        worker,
        "--master-id",
        cfg["master_id"],
        "--account",
        cfg["account"],
        "--password",
        cfg["password"],
        "--server",
        cfg["server"],
        "--mt5-path",
        cfg["mt5_path"],
        "--symbol",
        cfg["xau_symbol"],
        "--engine-host",
        ENGINE_HOST,
        "--engine-port",
        str(ENGINE_PORT),
    ]


def terminate_process(proc):
    try:
        proc.terminate()
        proc.wait(timeout=8)
    except Exception:
        try:
            proc.kill()
        except Exception:
            pass


def handle_signal(_sig, _frame):
    global RUNNING
    RUNNING = False


def main():
    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    children = {}  # master_id => {proc, cfg}

    print(f"[MASTER-MANAGER] API={API_BASE} ENGINE={ENGINE_HOST}:{ENGINE_PORT}")
    print("[MASTER-MANAGER] mt5_path_source=db.masterAccounts[].mt5Path")

    while RUNNING:
        try:
            data = fetch_db()
            desired = {}
            for master in data.get("masterAccounts", []):
                cfg = normalize_master(master)
                if cfg:
                    desired[cfg["master_id"]] = cfg

            current_ids = set(children.keys())
            desired_ids = set(desired.keys())

            for master_id in sorted(current_ids - desired_ids):
                print(f"[MASTER-MANAGER] stopping master {master_id} (inactive/removed)")
                terminate_process(children[master_id]["proc"])
                del children[master_id]

            for master_id in sorted(desired_ids):
                cfg = desired[master_id]
                running = children.get(master_id)
                if running:
                    proc = running["proc"]
                    if proc.poll() is None and running["cfg"] == cfg:
                        continue
                    print(f"[MASTER-MANAGER] restarting master {master_id}")
                    terminate_process(proc)
                    del children[master_id]

                cmd = build_command(cfg)
                proc = subprocess.Popen(cmd)
                children[master_id] = {"proc": proc, "cfg": cfg}
                print(
                    f"[MASTER-MANAGER] started master={master_id} pid={proc.pid} "
                    f"account={cfg['account']} server={cfg['server']}"
                )

        except Exception as e:
            print(f"[MASTER-MANAGER] loop error: {e}")

        for _ in range(POLL_SECONDS):
            if not RUNNING:
                break
            time.sleep(1)

    print("[MASTER-MANAGER] stopping all workers...")
    for rec in list(children.values()):
        terminate_process(rec["proc"])
    print("[MASTER-MANAGER] stopped")


if __name__ == "__main__":
    main()
