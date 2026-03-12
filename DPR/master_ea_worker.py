# -*- coding: utf-8 -*-
"""
One master EA worker process per active master account.
Connects to MT5 and DPR engine, sends account data, and emits reverse-copy signals.
"""

import argparse
import json
import socket
import threading
import time
import sys

try:
    import MetaTrader5 as mt5
except ImportError:
    print("[ERROR] MetaTrader5 package not installed. Run: pip install MetaTrader5")
    sys.exit(1)

DATA_INTERVAL = 10
RECONNECT_BACKOFF = 5

mt5_lock = threading.Lock()

HEDGE_COMMENT_PREFIX = "RG:"
_slave_hedge_lock = threading.Lock()
_slave_hedge_map = {}  # {(slaveId, slaveTicket): masterTicket}

_pending_hedge_lock = threading.Lock()
_pending_hedge_opens = set()  # {comment str} — hedge opens currently in-flight, prevents duplicates


def tcp_send(sock, line):
    sock.sendall((line.strip() + "\n").encode("utf-8"))


def read_line(sock, timeout=5):
    sock.settimeout(timeout)
    buf = b""
    try:
        while b"\n" not in buf:
            chunk = sock.recv(256)
            if not chunk:
                raise ConnectionError("Engine closed connection")
            buf += chunk
    finally:
        sock.settimeout(None)
    return buf.split(b"\n")[0].decode("utf-8", errors="ignore").strip()


def connect_mt5(account, password, server, mt5_path, instance_id):
    with mt5_lock:
        ok = mt5.initialize(
            path=mt5_path,
            login=int(account),
            password=password,
            server=server,
            timeout=30000,
        )
        if not ok:
            err = mt5.last_error()
            print(f"[{instance_id}] MT5 init/login failed: {err}")
            mt5.shutdown()
            return False, f"MT5 init/login failed: {err}"

        info = mt5.account_info()
        if info is None:
            mt5.shutdown()
            return False, "MT5 account_info returned None after login"

        terminal = mt5.terminal_info()
        print(
            f"[{instance_id}] MT5 connected - login={info.login} "
            f"balance={info.balance} equity={info.equity} "
            f"trade_allowed={terminal.trade_allowed if terminal else '?'}"
        )
        return True, "Connected"


def get_account_snapshot():
    with mt5_lock:
        info = mt5.account_info()
        if info is None:
            return None
        positions = mt5.positions_get() or []
        day_pnl = sum(p.profit for p in positions)
        return {
            "balance": round(info.balance, 2),
            "equity": round(info.equity, 2),
            "margin": round(info.margin, 2),
            "freeMargin": round(info.margin_free, 2),
            "pnl": round(day_pnl, 2),
        }


def send_status(sock, state, message=""):
    payload = json.dumps({"state": state, "message": message})
    tcp_send(sock, f"STATUS {payload}")
    print(f"[EA->Engine] STATUS state={state} message={message}")


def send_data(sock, snapshot):
    payload = json.dumps(snapshot)
    tcp_send(sock, f"DATA {payload}")
    print(f"[EA->Engine] DATA {payload}")


def send_signal(sock, payload):
    tcp_send(sock, f"SIGNAL {json.dumps(payload)}")
    print(f"[EA->Engine] SIGNAL {payload}")


def bind_account(sock, kind, account_id):
    payload = json.dumps({"kind": kind, "id": account_id})
    tcp_send(sock, f"BIND {payload}")
    resp = read_line(sock, timeout=5)
    if not resp.startswith("BOUND"):
        raise ConnectionError(f"Engine rejected account bind: {resp}")
    print(f"[EA->Engine] {resp}")


def register_with_engine(sock, broker, instance_id, master_id):
    tcp_send(sock, instance_id)
    resp = read_line(sock, timeout=5)
    if resp != "VALID":
        raise ConnectionError(f"Engine rejected identification: {resp}")
    print(f"[{instance_id}] Engine accepted connection")

    tcp_send(sock, f"BROKER {broker}")
    resp = read_line(sock, timeout=5)
    if not resp.startswith("REGISTERED"):
        raise ConnectionError(f"Engine rejected broker tag: {resp}")
    print(f"[{instance_id}] {resp}")

    bind_account(sock, "master", master_id)


def order_send_with_fill_fallback(base_request):
    for fill in (mt5.ORDER_FILLING_FOK, mt5.ORDER_FILLING_IOC, mt5.ORDER_FILLING_RETURN):
        req = dict(base_request)
        req["type_filling"] = fill
        try:
            result = mt5.order_send(req)
        except Exception:
            continue
        if result and result.retcode == mt5.TRADE_RETCODE_DONE:
            return result
        if result and result.retcode in (
            mt5.TRADE_RETCODE_INVALID_FILL,
            mt5.TRADE_RETCODE_INVALID,
            mt5.TRADE_RETCODE_INVALID_VOLUME,
            mt5.TRADE_RETCODE_INVALID_PRICE,
        ):
            continue
        return result
    return None


def normalize_volume(symbol_info, volume):
    v = float(volume)
    vmin = float(getattr(symbol_info, "volume_min", 0.01) or 0.01)
    vmax = float(getattr(symbol_info, "volume_max", 100.0) or 100.0)
    step = float(getattr(symbol_info, "volume_step", 0.01) or 0.01)
    if v <= 0:
        return 0.0
    v = min(max(v, vmin), vmax)
    units = round(v / step)
    v = units * step
    digits = max(0, len(str(step).split(".")[1]) if "." in str(step) else 0)
    return round(v, min(digits, 8))


def open_slave_hedge(symbol, action, lot, comment, slave_id_src, slave_ticket, instance_id):
    """Open an opposite-direction hedge on master for a slave-originated position."""
    result = None
    try:
        with mt5_lock:
            if not mt5.symbol_select(symbol, True):
                print(f"[{instance_id}] HEDGE_OPEN symbol_select failed for {symbol}")
                return
            info = mt5.symbol_info(symbol)
            if info is None:
                print(f"[{instance_id}] HEDGE_OPEN no symbol info for {symbol}")
                return
            tick = mt5.symbol_info_tick(symbol)
            if tick is None:
                print(f"[{instance_id}] HEDGE_OPEN no tick for {symbol}")
                return
            vol = normalize_volume(info, lot)
            if vol <= 0:
                print(f"[{instance_id}] HEDGE_OPEN invalid volume for {symbol} lot={lot}")
                return
            order_type = mt5.ORDER_TYPE_BUY if action == "BUY" else mt5.ORDER_TYPE_SELL
            price = tick.ask if action == "BUY" else tick.bid
            request = {
                "action": mt5.TRADE_ACTION_DEAL,
                "symbol": symbol,
                "volume": vol,
                "type": order_type,
                "price": price,
                "magic": 5430,
                "comment": comment,
            }
            result = order_send_with_fill_fallback(request)

        if result and result.retcode == mt5.TRADE_RETCODE_DONE:
            print(f"[{instance_id}] HEDGE_OPEN_OK {action} {vol} {symbol} ticket={result.order} comment={comment}")
            with _slave_hedge_lock:
                _slave_hedge_map[(str(slave_id_src), int(slave_ticket))] = int(result.order)
        else:
            code = result.retcode if result else "none"
            cmt = result.comment if result else ""
            print(f"[{instance_id}] HEDGE_OPEN_FAIL {action} {vol} {symbol} code={code} {cmt}")
    finally:
        with _pending_hedge_lock:
            _pending_hedge_opens.discard(comment)


def close_slave_hedge(comment, instance_id):
    """Close the master hedge position that matches the given comment."""
    with mt5_lock:
        positions = mt5.positions_get() or []
        target = None
        for p in positions:
            if getattr(p, "comment", "") == comment:
                target = p
                break

        if target is None:
            print(f"[{instance_id}] HEDGE_CLOSE no position found comment={comment}")
            return

        is_buy = target.type == mt5.POSITION_TYPE_BUY
        close_type = mt5.ORDER_TYPE_SELL if is_buy else mt5.ORDER_TYPE_BUY
        tick = mt5.symbol_info_tick(target.symbol)
        if tick is None:
            print(f"[{instance_id}] HEDGE_CLOSE no tick for {target.symbol}")
            return
        price = tick.bid if is_buy else tick.ask

        request = {
            "action": mt5.TRADE_ACTION_DEAL,
            "symbol": target.symbol,
            "volume": float(target.volume),
            "type": close_type,
            "position": int(target.ticket),
            "price": price,
            "magic": 5430,
            "comment": "RG:X",
        }
        result = order_send_with_fill_fallback(request)

    if result and result.retcode == mt5.TRADE_RETCODE_DONE:
        print(f"[{instance_id}] HEDGE_CLOSE_OK ticket={target.ticket} comment={comment}")
    else:
        code = result.retcode if result else "none"
        cmt = result.comment if result else ""
        print(f"[{instance_id}] HEDGE_CLOSE_FAIL ticket={target.ticket} code={code} {cmt}")


def close_master_position_by_ticket(ticket, volume_to_close, instance_id):
    with mt5_lock:
        positions = mt5.positions_get(ticket=int(ticket))
        if not positions:
            print(f"[{instance_id}] SLAVE_CLOSE_REQ ticket={ticket} not found (already closed?)")
            return
        pos = positions[0]
        requested_volume = float(pos.volume) if volume_to_close is None else float(volume_to_close)
        if requested_volume <= 0:
            print(f"[{instance_id}] SLAVE_CLOSE_REQ ticket={ticket} ignored invalid volume={requested_volume}")
            return
        close_volume = min(float(pos.volume), requested_volume)
        info = mt5.symbol_info(pos.symbol)
        if info is None:
            print(f"[{instance_id}] SLAVE_CLOSE_REQ no symbol info for {pos.symbol}")
            return
        close_volume = normalize_volume(info, close_volume)
        if close_volume <= 0:
            print(f"[{instance_id}] SLAVE_CLOSE_REQ ticket={ticket} normalized volume invalid")
            return
        is_buy = pos.type == mt5.POSITION_TYPE_BUY
        close_type = mt5.ORDER_TYPE_SELL if is_buy else mt5.ORDER_TYPE_BUY
        tick = mt5.symbol_info_tick(pos.symbol)
        if tick is None:
            print(f"[{instance_id}] SLAVE_CLOSE_REQ no tick for {pos.symbol}")
            return
        price = tick.bid if is_buy else tick.ask
        request = {
            "action": mt5.TRADE_ACTION_DEAL,
            "symbol": pos.symbol,
            "volume": close_volume,
            "type": close_type,
            "position": int(ticket),
            "price": price,
            "magic": 5430,
            "comment": "EC",
        }
        result = order_send_with_fill_fallback(request)
        if result and result.retcode == mt5.TRADE_RETCODE_DONE:
            print(f"[{instance_id}] SLAVE_CLOSE_OK ticket={ticket} vol={close_volume}")
        else:
            code = result.retcode if result else "none"
            cmt = result.comment if result else ""
            print(f"[{instance_id}] SLAVE_CLOSE_FAIL ticket={ticket} vol={close_volume} code={code} {cmt}")


def start_listener(sock, stop_event, instance_id):
    def run():
        buffer = ""
        while not stop_event.is_set():
            try:
                chunk = sock.recv(1024)
                if not chunk:
                    stop_event.set()
                    break
                buffer += chunk.decode("utf-8", errors="ignore")
                while "\n" in buffer:
                    idx = buffer.index("\n")
                    line = buffer[:idx].strip()
                    buffer = buffer[idx + 1:]
                    if not line:
                        continue
                    try:
                        data = json.loads(line)
                        if data.get("kind") == "slave_origin_open":
                            slave_id_src = str(data.get("slaveId") or "")
                            slave_ticket = int(data.get("slaveTicket") or 0)
                            symbol = str(data.get("symbol") or "")
                            action = str(data.get("action") or "").upper()
                            lot = float(data.get("lot") or 0.01)
                            hedge_action = "SELL" if action == "BUY" else "BUY"
                            comment = f"{HEDGE_COMMENT_PREFIX}{slave_id_src}:{slave_ticket}"
                            if slave_id_src and slave_ticket and symbol and action in ("BUY", "SELL"):
                                # Atomic duplicate + in-flight guard
                                with _pending_hedge_lock:
                                    already_pending = comment in _pending_hedge_opens
                                    if not already_pending:
                                        _pending_hedge_opens.add(comment)
                                if already_pending:
                                    print(f"[{instance_id}] SLAVE_ORIGIN_OPEN in-flight, skipped comment={comment}")
                                else:
                                    # Also verify not already open in MT5
                                    with mt5_lock:
                                        existing = mt5.positions_get() or []
                                        already_open = any(getattr(p, "comment", "") == comment for p in existing)
                                    if already_open:
                                        with _pending_hedge_lock:
                                            _pending_hedge_opens.discard(comment)
                                        print(f"[{instance_id}] SLAVE_ORIGIN_OPEN already in MT5, skipped comment={comment}")
                                    else:
                                        print(f"[{instance_id}] SLAVE_ORIGIN_OPEN slaveId={slave_id_src} ticket={slave_ticket} → hedge {hedge_action} {lot} {symbol}")
                                        threading.Thread(
                                            target=open_slave_hedge,
                                            args=(symbol, hedge_action, lot, comment, slave_id_src, slave_ticket, instance_id),
                                            daemon=True,
                                        ).start()
                            continue
                        if data.get("kind") == "slave_origin_close":
                            slave_id_src = str(data.get("slaveId") or "")
                            slave_ticket = int(data.get("slaveTicket") or 0)
                            comment = f"{HEDGE_COMMENT_PREFIX}{slave_id_src}:{slave_ticket}"
                            if slave_id_src and slave_ticket:
                                # Cancel any in-flight open for this position
                                with _pending_hedge_lock:
                                    _pending_hedge_opens.discard(comment)
                                print(f"[{instance_id}] SLAVE_ORIGIN_CLOSE slaveId={slave_id_src} ticket={slave_ticket} → closing hedge")
                                threading.Thread(
                                    target=close_slave_hedge,
                                    args=(comment, instance_id),
                                    daemon=True,
                                ).start()
                            continue
                        if data.get("kind") == "slave_close":
                            ticket = int(data.get("masterTicket") or 0)
                            close_volume = data.get("closedVolume")
                            if close_volume is not None:
                                try:
                                    close_volume = float(close_volume)
                                except Exception:
                                    close_volume = None
                            if ticket:
                                print(
                                    f"[{instance_id}] SLAVE_CLOSE_REQ slaveId={data.get('slaveId')} "
                                    f"ticket={ticket} vol={close_volume if close_volume is not None else 'full'}"
                                )
                                threading.Thread(
                                    target=close_master_position_by_ticket,
                                    args=(ticket, close_volume, instance_id),
                                    daemon=True,
                                ).start()
                    except Exception as e:
                        print(f"[{instance_id}] listener parse error: {e} line={line!r}")
            except Exception:
                stop_event.set()
                break

    threading.Thread(target=run, daemon=True).start()


def start_ping(sock, stop_event, instance_id):
    def run():
        while not stop_event.wait(30):
            try:
                sock.sendall(b"\n")
            except Exception as e:
                print(f"[{instance_id}] Ping error: {e}")
                break

    threading.Thread(target=run, daemon=True).start()


def start_data_sender(sock, stop_event, instance_id):
    def run():
        while not stop_event.wait(DATA_INTERVAL):
            try:
                snap = get_account_snapshot()
                if snap:
                    send_data(sock, snap)
                else:
                    print(f"[{instance_id}] MT5 account_info unavailable - skipping DATA")
            except Exception as e:
                print(f"[{instance_id}] Data sender error: {e}")
                break

    threading.Thread(target=run, daemon=True).start()


def start_reverse_copy_monitor(sock, stop_event, instance_id, master_id):
    def run():
        known = {}
        last_snapshot_at = 0.0
        while not stop_event.wait(2):
            try:
                with mt5_lock:
                    positions = mt5.positions_get() or []

                current = {}
                for p in positions:
                    # Exclude hedge positions — slaves must never copy or track them
                    if (getattr(p, "comment", "") or "").startswith(HEDGE_COMMENT_PREFIX):
                        continue
                    ticket = int(p.ticket)
                    current[ticket] = {
                        "type": int(p.type),
                        "volume": float(p.volume),
                        "symbol": p.symbol,
                    }

                now = time.time()
                if now - last_snapshot_at >= 5:
                    send_signal(
                        sock,
                        {
                            "kind": "reverse_copy",
                            "masterId": master_id,
                            "op": "snapshot",
                            "positions": {str(t): round(v["volume"], 8) for t, v in current.items()},
                        },
                    )
                    last_snapshot_at = now

                for ticket in list(known.keys()):
                    if ticket not in current:
                        send_signal(
                            sock,
                            {
                                "kind": "reverse_copy",
                                "masterId": master_id,
                                "op": "close",
                                "masterTicket": ticket,
                            },
                        )

                for ticket, cur in current.items():
                    prev = known.get(ticket)
                    reverse_action = "SELL" if cur["type"] == mt5.POSITION_TYPE_BUY else "BUY"

                    if prev is None:
                        send_signal(
                            sock,
                            {
                                "kind": "reverse_copy",
                                "masterId": master_id,
                                "op": "open",
                                "masterTicket": ticket,
                                "symbol": cur["symbol"],
                                "action": reverse_action,
                                "lot": round(cur["volume"], 8),
                            },
                        )
                        continue

                    if prev["type"] != cur["type"] or prev["symbol"] != cur["symbol"]:
                        send_signal(
                            sock,
                            {
                                "kind": "reverse_copy",
                                "masterId": master_id,
                                "op": "close",
                                "masterTicket": ticket,
                            },
                        )
                        send_signal(
                            sock,
                            {
                                "kind": "reverse_copy",
                                "masterId": master_id,
                                "op": "open",
                                "masterTicket": ticket,
                                "symbol": cur["symbol"],
                                "action": reverse_action,
                                "lot": round(cur["volume"], 8),
                            },
                        )
                        continue

                    delta = round(cur["volume"] - prev["volume"], 8)
                    if delta > 1e-8:
                        send_signal(
                            sock,
                            {
                                "kind": "reverse_copy",
                                "masterId": master_id,
                                "op": "open",
                                "masterTicket": ticket,
                                "symbol": cur["symbol"],
                                "action": reverse_action,
                                "lot": delta,
                            },
                        )
                    elif delta < -1e-8:
                        send_signal(
                            sock,
                            {
                                "kind": "reverse_copy",
                                "masterId": master_id,
                                "op": "reduce",
                                "masterTicket": ticket,
                                "lot": abs(delta),
                            },
                        )

                known = current
            except Exception as e:
                print(f"[{instance_id}] Reverse copy monitor error: {e}")
                continue

    threading.Thread(target=run, daemon=True).start()


def parse_args():
    p = argparse.ArgumentParser(description="DPR Master EA worker")
    p.add_argument("--master-id", required=True)
    p.add_argument("--account", required=True)
    p.add_argument("--password", required=True)
    p.add_argument("--server", required=True)
    p.add_argument("--mt5-path", required=True)
    p.add_argument("--engine-host", default="127.0.0.1")
    p.add_argument("--engine-port", type=int, default=9090)
    return p.parse_args()


def main():
    args = parse_args()
    instance_id = f"master-{args.master_id}-{args.account}"
    backoff = RECONNECT_BACKOFF

    print("=" * 60)
    print(f"  DPR Master EA Worker - {instance_id}")
    print("=" * 60)

    while True:
        ok, msg = connect_mt5(args.account, args.password, args.server, args.mt5_path, instance_id)

        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
            sock.connect((args.engine_host, args.engine_port))
            print(f"[{instance_id}] TCP connected to engine {args.engine_host}:{args.engine_port}")
        except Exception as e:
            print(f"[{instance_id}] Cannot reach DPR Engine: {e}")
            time.sleep(backoff)
            continue

        try:
            register_with_engine(sock, args.server, instance_id, args.master_id)
        except Exception as e:
            print(f"[{instance_id}] Registration failed: {e}")
            sock.close()
            time.sleep(backoff)
            continue

        if ok:
            send_status(sock, "connected", msg)
        else:
            send_status(sock, "error", msg)
            try:
                while True:
                    time.sleep(60)
            except KeyboardInterrupt:
                pass
            finally:
                sock.close()
            time.sleep(backoff)
            continue

        stop_event = threading.Event()
        start_listener(sock, stop_event, instance_id)
        start_ping(sock, stop_event, instance_id)
        start_data_sender(sock, stop_event, instance_id)
        start_reverse_copy_monitor(sock, stop_event, instance_id, args.master_id)

        try:
            while True:
                time.sleep(5)
                with mt5_lock:
                    info = mt5.account_info()
                if info is None:
                    raise ConnectionError("MT5 account_info returned None - connection lost")
        except KeyboardInterrupt:
            print(f"\n[{instance_id}] Stopped by user")
            stop_event.set()
            try:
                send_status(sock, "disconnected", "Stopped by user")
            except Exception:
                pass
            sock.close()
            with mt5_lock:
                mt5.shutdown()
            sys.exit(0)
        except Exception as e:
            print(f"[{instance_id}] Connection lost: {e}")
            stop_event.set()
            try:
                send_status(sock, "disconnected", str(e))
            except Exception:
                pass
            sock.close()
            with mt5_lock:
                mt5.shutdown()

        print(f"[{instance_id}] Reconnecting in {backoff}s ...")
        time.sleep(backoff)


if __name__ == "__main__":
    main()
