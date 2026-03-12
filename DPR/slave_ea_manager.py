# -*- coding: utf-8 -*-
"""
Spawns one slave_ea_worker.py process per active slave account.
This avoids the MT5 limitation of one Python process handling multiple terminals/accounts.
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
POLL_SECONDS = int(os.environ.get("DPR_SLAVE_POLL_SECONDS", "10"))

RUNNING = True


def fetch_db():
    url = f"{API_BASE}/api/data"
    with urllib.request.urlopen(urllib.request.Request(url), timeout=8) as resp:
        return json.loads(resp.read().decode())


def normalize_slave(slave, active_masters):
    master = active_masters.get(slave.get("masterId"))
    if not master:
        return None

    slave_id = slave.get("id")
    account = str(slave.get("accountNumber") or "").strip()
    # Never strip password; leading/trailing spaces may be intentional.
    password = str(slave.get("masterPass") or "")
    server = (slave.get("broker") or "").strip()
    route_tag = (master.get("broker") or "").strip()

    if not slave_id or not account or not password or not server or not route_tag:
        return None

    mt5_path = (slave.get("mt5Path") or "").strip()
    if not mt5_path:
        return None
    return {
        "slave_id": str(slave_id),
        "master_id": str(slave.get("masterId")),
        "account": account,
        "password": password,
        "server": server,
        "route_tag": route_tag,
        "mt5_path": mt5_path,
    }


def build_command(cfg):
    worker = os.path.join(os.path.dirname(os.path.abspath(__file__)), "slave_ea_worker.py")
    return [
        sys.executable,
        worker,
        "--slave-id",
        cfg["slave_id"],
        "--master-id",
        cfg["master_id"],
        "--route-tag",
        cfg["route_tag"],
        "--account",
        cfg["account"],
        "--password",
        cfg["password"],
        "--server",
        cfg["server"],
        "--mt5-path",
        cfg["mt5_path"],
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

    children = {}  # slave_id => {proc, cfg}

    print(f"[SLAVE-MANAGER] API={API_BASE} ENGINE={ENGINE_HOST}:{ENGINE_PORT}")
    print("[SLAVE-MANAGER] mt5_path_source=db.slaveAccounts[].mt5Path")

    while RUNNING:
        try:
            data = fetch_db()
            active_masters = {
                m.get("id"): m
                for m in data.get("masterAccounts", [])
                if m.get("status") == "active" and m.get("id")
            }

            desired = {}
            for slave in data.get("slaveAccounts", []):
                cfg = normalize_slave(slave, active_masters)
                if cfg:
                    desired[cfg["slave_id"]] = cfg

            current_ids = set(children.keys())
            desired_ids = set(desired.keys())

            for slave_id in sorted(current_ids - desired_ids):
                print(f"[SLAVE-MANAGER] stopping slave {slave_id} (inactive/removed)")
                terminate_process(children[slave_id]["proc"])
                del children[slave_id]

            for slave_id in sorted(desired_ids):
                cfg = desired[slave_id]
                running = children.get(slave_id)
                if running:
                    proc = running["proc"]
                    if proc.poll() is None and running["cfg"] == cfg:
                        continue
                    print(f"[SLAVE-MANAGER] restarting slave {slave_id}")
                    terminate_process(proc)
                    del children[slave_id]

                cmd = build_command(cfg)
                proc = subprocess.Popen(cmd)
                children[slave_id] = {"proc": proc, "cfg": cfg}
                print(
                    f"[SLAVE-MANAGER] started slave={slave_id} pid={proc.pid} "
                    f"account={cfg['account']} route_tag={cfg['route_tag']}"
                )

        except Exception as e:
            print(f"[SLAVE-MANAGER] loop error: {e}")

        for _ in range(POLL_SECONDS):
            if not RUNNING:
                break
            time.sleep(1)

    print("[SLAVE-MANAGER] stopping all workers...")
    for rec in list(children.values()):
        terminate_process(rec["proc"])
    print("[SLAVE-MANAGER] stopped")


if __name__ == "__main__":
    main()
