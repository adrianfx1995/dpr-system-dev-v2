# -*- coding: utf-8 -*-
"""
One slave EA process per MT5 account.
Receives trade commands from DPR engine TCP and executes them on MT5.
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
PING_INTERVAL = 30
VOLUME_EPS = 1e-6

mt5_lock = threading.Lock()
COPY_COMMENT_PREFIX = "SG:"

_state_lock = threading.Lock()
_last_master_positions = {}  # {int(masterTicket): volume} — updated each snapshot
_master_driven_close_credit = {}  # {int(masterTicket): volume} already explained by master->slave close/reduce
_upstream_close_pending = {}  # {int(masterTicket): volume} already reported upstream, waiting for master snapshot ack
_last_slave_copy_volumes = {}  # {int(masterTicket): volume} last observed slave copied volume

_slave_origin_lock = threading.Lock()
_slave_origin_positions = {}  # {int(slaveTicket): {"symbol":…, "action":…, "lot":…}}


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
            return False, "MT5 account_info returned None"

        terminal = mt5.terminal_info()
        print(
            f"[{instance_id}] MT5 connected login={info.login} server={info.server} "
            f"trade_allowed={terminal.trade_allowed if terminal else '?'}"
        )
        return True, "Connected"


def get_snapshot():
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
    tcp_send(sock, f"STATUS {json.dumps({'state': state, 'message': message})}")


def send_data(sock, payload):
    tcp_send(sock, f"DATA {json.dumps(payload)}")


def register_with_engine(sock, instance_id, route_tag, slave_id, master_id):
    tcp_send(sock, instance_id)
    resp = read_line(sock)
    if resp != "VALID":
        raise ConnectionError(f"invalid handshake response: {resp}")

    tcp_send(sock, f"BROKER {route_tag}")
    resp = read_line(sock)
    if not resp.startswith("REGISTERED"):
        raise ConnectionError(f"invalid register response: {resp}")

    tcp_send(sock, f"BIND {json.dumps({'kind': 'slave', 'id': slave_id, 'masterId': master_id})}")
    resp = read_line(sock)
    if not resp.startswith("BOUND"):
        raise ConnectionError(f"invalid bind response: {resp}")


def execute_trade(symbol, action, lot, instance_id):
    with mt5_lock:
        req = build_market_request(symbol, action, lot, "EA", None, instance_id)
        if not req:
            return
        result = order_send_with_fill_fallback(req)
        if result and result.retcode == mt5.TRADE_RETCODE_DONE:
            print(f"[{instance_id}] TRADE_OK {action} {req['volume']} {symbol}")
        else:
            code = result.retcode if result else "none"
            comment = result.comment if result else "no result"
            print(f"[{instance_id}] TRADE_FAIL {action} code={code} comment={comment}")


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


def build_market_request(symbol, action, lot, comment, position, instance_id):
    if not mt5.symbol_select(symbol, True):
        print(f"[{instance_id}] TRADE_ERROR symbol_select failed {symbol}")
        return None
    info = mt5.symbol_info(symbol)
    if info is None:
        print(f"[{instance_id}] TRADE_ERROR no symbol info {symbol}")
        return None
    tick = mt5.symbol_info_tick(symbol)
    if tick is None:
        print(f"[{instance_id}] TRADE_ERROR no tick {symbol}")
        return None

    volume = normalize_volume(info, lot)
    if volume <= 0:
        print(f"[{instance_id}] TRADE_ERROR invalid normalized volume {lot} -> {volume} for {symbol}")
        return None

    order_type = mt5.ORDER_TYPE_BUY if action == "BUY" else mt5.ORDER_TYPE_SELL
    price = tick.ask if action == "BUY" else tick.bid
    if not price or price <= 0:
        print(f"[{instance_id}] TRADE_ERROR invalid price {symbol}")
        return None

    req = {
        "action": mt5.TRADE_ACTION_DEAL,
        "symbol": symbol,
        "volume": volume,
        "type": order_type,
        "price": price,
        "sl": 0.0,
        "tp": 0.0,
        "magic": 5430,
        "comment": comment,
    }
    if position is not None:
        req["position"] = position
    return req


def order_send_with_fill_fallback(base_request):
    # Different brokers/accounts accept different filling modes.
    for fill in (mt5.ORDER_FILLING_FOK, mt5.ORDER_FILLING_IOC, mt5.ORDER_FILLING_RETURN):
        req = dict(base_request)
        req["type_filling"] = fill
        try:
            result = mt5.order_send(req)
        except Exception:
            continue
        if result and result.retcode == mt5.TRADE_RETCODE_DONE:
            return result
        # If request itself is invalid for this fill mode, try next.
        if result and result.retcode in (
            mt5.TRADE_RETCODE_INVALID_FILL,
            mt5.TRADE_RETCODE_INVALID,
            mt5.TRADE_RETCODE_INVALID_VOLUME,
            mt5.TRADE_RETCODE_INVALID_PRICE,
        ):
            continue
        return result
    return None


def execute_copy_open(symbol, action, lot, master_ticket, instance_id):
    with mt5_lock:
        marker = f"{COPY_COMMENT_PREFIX}{master_ticket}"
        existing = mt5.positions_get() or []
        for p in existing:
            if getattr(p, "comment", "") == marker:
                # Prevent duplicate opens on EA reconnect/restart
                return

        request = build_market_request(symbol, action, lot, marker, None, instance_id)
        if not request:
            return
        result = order_send_with_fill_fallback(request)
        if result and result.retcode == mt5.TRADE_RETCODE_DONE:
            print(f"[{instance_id}] COPY_OPEN ok masterTicket={master_ticket} action={action} lot={request['volume']} {symbol}")
        else:
            code = result.retcode if result else "none"
            comment = result.comment if result else "no result"
            print(f"[{instance_id}] COPY_OPEN fail masterTicket={master_ticket} code={code} comment={comment}")


def close_copy_by_master_ticket(master_ticket, volume_to_close, instance_id):
    max_retries = 3
    marker = f"{COPY_COMMENT_PREFIX}{master_ticket}"
    closed_total = 0.0
    remaining_total = float(volume_to_close) if volume_to_close is not None else None
    for attempt in range(max_retries):
        with mt5_lock:
            if remaining_total is not None and remaining_total <= VOLUME_EPS:
                return round(closed_total, 8)
            positions = [p for p in (mt5.positions_get() or []) if getattr(p, "comment", "") == marker]
            if not positions:
                return round(closed_total, 8)  # All closed

            all_ok = True
            for pos in positions:
                if remaining_total is not None and remaining_total <= VOLUME_EPS:
                    break
                is_buy = pos.type in (mt5.POSITION_TYPE_BUY, 0)
                close_action = "SELL" if is_buy else "BUY"
                close_volume = float(pos.volume)
                if remaining_total is not None:
                    close_volume = min(close_volume, remaining_total)
                request = build_market_request(
                    pos.symbol, close_action, close_volume,
                    f"{marker}:X", pos.ticket, instance_id,
                )
                if not request:
                    all_ok = False
                    continue
                result = order_send_with_fill_fallback(request)
                if result and result.retcode == mt5.TRADE_RETCODE_DONE:
                    print(f"[{instance_id}] COPY_CLOSE ok masterTicket={master_ticket} vol={request['volume']}")
                    closed_total = round(closed_total + float(request["volume"]), 8)
                    if remaining_total is not None:
                        remaining_total = round(remaining_total - request["volume"], 8)
                else:
                    all_ok = False
                    code = result.retcode if result else "none"
                    comment = result.comment if result else "no result"
                    print(f"[{instance_id}] COPY_CLOSE fail attempt={attempt+1}/{max_retries} masterTicket={master_ticket} code={code} {comment}")

        if all_ok:
            return round(closed_total, 8)
        if attempt < max_retries - 1:
            time.sleep(1.0)  # Wait for fresh prices before retry

    print(f"[{instance_id}] COPY_CLOSE gave_up masterTicket={master_ticket} — snapshot reconciler will retry")
    return round(closed_total, 8)


def record_master_driven_close(master_ticket, closed_volume):
    vol = round(float(closed_volume or 0.0), 8)
    if vol <= VOLUME_EPS:
        return
    ticket = int(master_ticket)
    with _state_lock:
        prev = _master_driven_close_credit.get(ticket, 0.0)
        _master_driven_close_credit[ticket] = round(prev + vol, 8)


def send_reverse_signal(sock, slave_id, master_id, master_ticket, closed_volume):
    vol = round(float(closed_volume), 8)
    payload = json.dumps({
        "kind": "slave_close",
        "slaveId": slave_id,
        "masterId": master_id,
        "masterTicket": master_ticket,
        "closedVolume": vol,
    })
    tcp_send(sock, f"REVERSE_SIGNAL {payload}")
    print(f"[slave-{slave_id}] SLAVE_CLOSE_UPSTREAM masterTicket={master_ticket} vol={vol}")


def start_slave_position_monitor(sock, slave_id, master_id, instance_id, stop_event):
    def run():
        while not stop_event.wait(2):
            try:
                with mt5_lock:
                    positions = mt5.positions_get()
                if positions is None:
                    continue  # MT5 glitch — skip, don't report false volume drops
                current_volumes = {}  # {int(masterTicket): slave copied volume}
                for p in positions:
                    comment = getattr(p, "comment", "") or ""
                    if not comment.startswith(COPY_COMMENT_PREFIX):
                        continue
                    raw = comment[len(COPY_COMMENT_PREFIX):].split("-", 1)[0]
                    try:
                        ticket = int(raw)
                        current_volumes[ticket] = round(current_volumes.get(ticket, 0.0) + float(p.volume), 8)
                    except Exception:
                        pass

                to_report = []
                with _state_lock:
                    expected_master = dict(_last_master_positions)
                    expected_tickets = set(expected_master.keys())
                    previous_volumes = dict(_last_slave_copy_volumes)
                    for ticket in list(_master_driven_close_credit.keys()):
                        if ticket not in expected_tickets:
                            _master_driven_close_credit.pop(ticket, None)
                    for ticket in list(_upstream_close_pending.keys()):
                        if ticket not in expected_tickets:
                            _upstream_close_pending.pop(ticket, None)

                    # Only report slave->master closes when copied volume actually decreased
                    # on the slave side. This avoids false master closes when a copy-open fails.
                    for ticket, previous_volume in previous_volumes.items():
                        current_volume = current_volumes.get(ticket, 0.0)
                        local_reduction = round(float(previous_volume) - float(current_volume), 8)
                        if local_reduction <= VOLUME_EPS:
                            continue

                        credit = _master_driven_close_credit.get(ticket, 0.0)
                        if credit > VOLUME_EPS:
                            used = min(local_reduction, credit)
                            local_reduction = round(local_reduction - used, 8)
                            credit = round(credit - used, 8)
                            if credit <= VOLUME_EPS:
                                _master_driven_close_credit.pop(ticket, None)
                            else:
                                _master_driven_close_credit[ticket] = credit

                        if local_reduction <= VOLUME_EPS:
                            continue

                        already_sent = _upstream_close_pending.get(ticket, 0.0)
                        _upstream_close_pending[ticket] = round(already_sent + local_reduction, 8)
                        to_report.append((ticket, local_reduction))

                    _last_slave_copy_volumes.clear()
                    for ticket, volume in current_volumes.items():
                        if ticket in expected_tickets:
                            _last_slave_copy_volumes[ticket] = round(float(volume), 8)

                for ticket, volume in to_report:
                    send_reverse_signal(sock, slave_id, master_id, ticket, volume)
            except Exception as e:
                print(f"[{instance_id}] slave_monitor error: {e}")

    threading.Thread(target=run, daemon=True).start()


def reconcile_copy_positions(master_positions, instance_id):
    """
    master_positions: dict {str(ticket): volume} from snapshot signal.
    Closes positions whose ticket is gone (full close missed).
    Reduces positions whose slave volume exceeds master volume (partial reduce missed).
    Also updates _last_master_positions for the reverse-close monitor and
    acknowledges already-reported slave closes once reflected on master snapshot.
    """
    global _last_master_positions
    with _state_lock:
        new_map = {int(t): float(v) for t, v in master_positions.items()}
        old_map = _last_master_positions

        # Safety guard: if snapshot suddenly shows 0 positions but we previously
        # knew master had open positions, this is almost certainly an MT5 API glitch
        # on the master side. Skip the stale-close to prevent false closures.
        # Individual op:close signals handle legitimate closes in real time.
        if len(new_map) == 0 and len(old_map) > 0:
            print(f"[{instance_id}] RECONCILE_SKIP empty snapshot while master had {len(old_map)} known positions — MT5 glitch guard")
            return
        for ticket, old_volume in old_map.items():
            new_volume = float(new_map.get(ticket, 0.0))
            reduced_on_master = round(float(old_volume) - new_volume, 8)
            if reduced_on_master > VOLUME_EPS:
                pending = round(_upstream_close_pending.get(ticket, 0.0) - reduced_on_master, 8)
                if pending <= VOLUME_EPS:
                    _upstream_close_pending.pop(ticket, None)
                else:
                    _upstream_close_pending[ticket] = pending

        keep_tickets = set(new_map.keys())
        for ticket in list(_master_driven_close_credit.keys()):
            if ticket not in keep_tickets:
                _master_driven_close_credit.pop(ticket, None)
        for ticket in list(_upstream_close_pending.keys()):
            if ticket not in keep_tickets:
                _upstream_close_pending.pop(ticket, None)

        _last_master_positions = new_map

    with mt5_lock:
        keep = {int(t): float(v) for t, v in master_positions.items()}
        positions = mt5.positions_get()
        if positions is None:
            print(f"[{instance_id}] RECONCILE_SKIP positions_get() returned None — MT5 glitch guard")
            return
        stale = []
        excess = []
        for p in positions:
            comment = getattr(p, "comment", "") or ""
            if not comment.startswith(COPY_COMMENT_PREFIX):
                continue
            raw = comment[len(COPY_COMMENT_PREFIX):].split("-", 1)[0]
            try:
                master_ticket = int(raw)
            except Exception:
                continue
            if master_ticket not in keep:
                stale.append(master_ticket)
            else:
                master_vol = keep[master_ticket]
                slave_vol = float(p.volume)
                over = round(slave_vol - master_vol, 8)
                if over > 0.001:  # slave carries more volume than master — reduce excess
                    excess.append((master_ticket, over))

    for ticket in sorted(set(stale)):
        print(f"[{instance_id}] RECONCILE_CLOSE masterTicket={ticket}")
        closed = close_copy_by_master_ticket(ticket, None, instance_id)
        record_master_driven_close(ticket, closed)

    for ticket, vol in excess:
        print(f"[{instance_id}] RECONCILE_REDUCE masterTicket={ticket} excess={vol}")
        closed = close_copy_by_master_ticket(ticket, vol, instance_id)
        record_master_driven_close(ticket, closed)


def close_positions(filter_fn, label, instance_id):
    with mt5_lock:
        positions = mt5.positions_get() or []
        if not positions:
            print(f"[{instance_id}] {label} no positions")
            return

        matched = 0
        for pos in positions:
            if not filter_fn(pos):
                continue
            if not mt5.symbol_select(pos.symbol, True):
                continue
            tick = mt5.symbol_info_tick(pos.symbol)
            if tick is None:
                continue

            is_buy = pos.type in (mt5.POSITION_TYPE_BUY, 0)
            opposite_type = mt5.ORDER_TYPE_SELL if is_buy else mt5.ORDER_TYPE_BUY
            price = tick.bid if is_buy else tick.ask
            if not price or price <= 0:
                continue

            request = {
                "action": mt5.TRADE_ACTION_DEAL,
                "symbol": pos.symbol,
                "volume": pos.volume,
                "type": opposite_type,
                "position": pos.ticket,
                "price": price,
                "type_filling": mt5.ORDER_FILLING_FOK,
                "magic": 5430,
                "comment": f"EA:{label}",
            }
            matched += 1
            result = mt5.order_send(request)
            if result and result.retcode == mt5.TRADE_RETCODE_DONE:
                print(f"[{instance_id}] {label} closed ticket={pos.ticket}")
            else:
                code = result.retcode if result else "none"
                print(f"[{instance_id}] {label} fail ticket={pos.ticket} code={code}")

        if matched == 0:
            print(f"[{instance_id}] {label} no matching positions")


def process_command(line, symbol, instance_id, master_id):
    try:
        data = json.loads(line)
    except json.JSONDecodeError:
        print(f"[{instance_id}] BAD_COMMAND {line}")
        return

    action = (data.get("action") or "").upper()
    lot = float(data.get("lot") or 0.01)

    if data.get("kind") == "reverse_copy":
        payload_master = str(data.get("masterId") or "")
        if payload_master and payload_master != str(master_id):
            return
        op = (data.get("op") or "").lower()
        if op == "snapshot":
            # New format: {str(ticket): volume}. Old format: list of tickets (backward compat).
            positions_map = data.get("positions")
            if positions_map is None:
                positions_map = {str(t): None for t in (data.get("tickets") or [])}
            # Replace None volumes with 0 so excess check is skipped (treat as full close only)
            positions_map = {k: (v if v is not None else 0.0) for k, v in positions_map.items()}
            reconcile_copy_positions(positions_map, instance_id)
            return
        master_ticket = int(data.get("masterTicket") or 0)
        if not master_ticket:
            print(f"[{instance_id}] COPY_BAD masterTicket missing: {data}")
            return
        if op == "open":
            copy_symbol = data.get("symbol") or symbol
            copy_action = (data.get("action") or "").upper()
            copy_lot = float(data.get("lot") or lot or 0.01)
            if copy_action in ("BUY", "SELL"):
                execute_copy_open(copy_symbol, copy_action, copy_lot, master_ticket, instance_id)
            return
        if op == "close":
            closed = close_copy_by_master_ticket(master_ticket, None, instance_id)
            record_master_driven_close(master_ticket, closed)
            return
        if op == "reduce":
            reduce_lot = float(data.get("lot") or 0.0)
            if reduce_lot > 0:
                closed = close_copy_by_master_ticket(master_ticket, reduce_lot, instance_id)
                record_master_driven_close(master_ticket, closed)
            return

    if action in ("BUY", "SELL"):
        execute_trade(symbol, action, lot, instance_id)
    elif action == "HEDGE":
        execute_trade(symbol, "BUY", lot, instance_id)
        execute_trade(symbol, "SELL", lot, instance_id)
    elif action == "CLOSE_ALL":
        close_positions(lambda _p: True, "CLOSE_ALL", instance_id)
    elif action == "CLOSE_BUYS":
        close_positions(lambda p: p.type in (mt5.POSITION_TYPE_BUY, 0), "CLOSE_BUYS", instance_id)
    elif action == "CLOSE_SELLS":
        close_positions(lambda p: p.type in (mt5.POSITION_TYPE_SELL, 1), "CLOSE_SELLS", instance_id)
    elif action == "CLOSE_PROFITS":
        close_positions(lambda p: p.profit > 0, "CLOSE_PROFITS", instance_id)
    else:
        print(f"[{instance_id}] UNKNOWN_ACTION {action}")


def start_slave_origin_monitor(sock, slave_id, master_id, instance_id, stop_event):
    """
    Monitors positions on the slave that were NOT opened by master copy (no DPR-MT: prefix).
    When a new slave-originated position is detected, signals master to open the opposite hedge.
    When it closes, signals master to close the hedge.
    """
    global _slave_origin_positions

    def run():
        global _slave_origin_positions
        while not stop_event.wait(0.5):
            try:
                with mt5_lock:
                    positions = mt5.positions_get() or []

                current = {}
                for p in positions:
                    comment = getattr(p, "comment", "") or ""
                    if comment.startswith(COPY_COMMENT_PREFIX):
                        continue  # skip master-copied positions
                    ticket = int(p.ticket)
                    action = "BUY" if p.type == mt5.POSITION_TYPE_BUY else "SELL"
                    current[ticket] = {
                        "symbol": p.symbol,
                        "action": action,
                        "lot": round(float(p.volume), 8),
                    }

                with _slave_origin_lock:
                    known = dict(_slave_origin_positions)

                # New positions: not in known
                for ticket, info in current.items():
                    if ticket not in known:
                        payload = json.dumps({
                            "kind": "slave_origin_open",
                            "slaveId": slave_id,
                            "masterId": master_id,
                            "slaveTicket": ticket,
                            "symbol": info["symbol"],
                            "action": info["action"],
                            "lot": info["lot"],
                        })
                        tcp_send(sock, f"REVERSE_SIGNAL {payload}")
                        print(f"[{instance_id}] SLAVE_ORIGIN_OPEN ticket={ticket} {info['action']} {info['lot']} {info['symbol']}")
                        with _slave_origin_lock:
                            _slave_origin_positions[ticket] = info
                    else:
                        prev = known[ticket]
                        changed = (
                            prev.get("symbol") != info["symbol"] or
                            prev.get("action") != info["action"] or
                            abs(float(prev.get("lot", 0.0)) - float(info["lot"])) > VOLUME_EPS
                        )
                        if changed:
                            payload = json.dumps({
                                "kind": "slave_origin_sync",
                                "slaveId": slave_id,
                                "masterId": master_id,
                                "slaveTicket": ticket,
                                "symbol": info["symbol"],
                                "action": info["action"],
                                "lot": info["lot"],
                            })
                            tcp_send(sock, f"REVERSE_SIGNAL {payload}")
                            print(f"[{instance_id}] SLAVE_ORIGIN_SYNC ticket={ticket} {info['action']} {info['lot']} {info['symbol']}")
                            with _slave_origin_lock:
                                _slave_origin_positions[ticket] = info

                # Closed positions: were known, now gone
                for ticket in list(known.keys()):
                    if ticket not in current:
                        payload = json.dumps({
                            "kind": "slave_origin_close",
                            "slaveId": slave_id,
                            "masterId": master_id,
                            "slaveTicket": ticket,
                        })
                        tcp_send(sock, f"REVERSE_SIGNAL {payload}")
                        print(f"[{instance_id}] SLAVE_ORIGIN_CLOSE ticket={ticket}")
                        with _slave_origin_lock:
                            _slave_origin_positions.pop(ticket, None)

            except Exception as e:
                print(f"[{instance_id}] slave_origin_monitor error: {e}")

    threading.Thread(target=run, daemon=True).start()


def start_listener(sock, symbol, instance_id, master_id, stop_event):
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
                    buffer = buffer[idx + 1 :]
                    if line:
                        process_command(line, symbol, instance_id, master_id)
            except Exception:
                stop_event.set()
                break

    t = threading.Thread(target=run, daemon=True)
    t.start()


def start_ping(sock, instance_id, stop_event):
    def run():
        while not stop_event.wait(PING_INTERVAL):
            try:
                sock.sendall(b"\n")
            except Exception:
                stop_event.set()
                break

    t = threading.Thread(target=run, daemon=True)
    t.start()


def parse_args():
    p = argparse.ArgumentParser(description="DPR Slave EA worker")
    p.add_argument("--slave-id", required=True)
    p.add_argument("--master-id", required=True)
    p.add_argument("--route-tag", required=True, help="Broker tag used to receive master commands")
    p.add_argument("--account", required=True)
    p.add_argument("--password", required=True)
    p.add_argument("--server", required=True)
    p.add_argument("--symbol", default="XAUUSD")
    p.add_argument("--mt5-path", required=True)
    p.add_argument("--engine-host", default="127.0.0.1")
    p.add_argument("--engine-port", type=int, default=9090)
    return p.parse_args()


def main():
    args = parse_args()
    instance_id = f"slave-{args.slave_id}-{args.account}"
    backoff = 5

    print(
        f"[{instance_id}] starting route_tag={args.route_tag} account={args.account} "
        f"server={args.server} symbol={args.symbol}"
    )

    while True:
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
            sock.connect((args.engine_host, args.engine_port))
            register_with_engine(sock, instance_id, args.route_tag, args.slave_id, args.master_id)
            send_status(sock, "connecting", "Connecting to MT5...")
        except Exception as e:
            print(f"[{instance_id}] engine connect/register failed: {e}")
            time.sleep(backoff)
            continue

        ok, msg = connect_mt5(args.account, args.password, args.server, args.mt5_path, instance_id)
        send_status(sock, "connected" if ok else "error", msg)
        if not ok:
            try:
                sock.close()
            except Exception:
                pass
            with mt5_lock:
                mt5.shutdown()
            time.sleep(backoff)
            continue

        stop_event = threading.Event()
        start_listener(sock, args.symbol, instance_id, args.master_id, stop_event)
        start_ping(sock, instance_id, stop_event)
        start_slave_position_monitor(sock, args.slave_id, args.master_id, instance_id, stop_event)
        start_slave_origin_monitor(sock, args.slave_id, args.master_id, instance_id, stop_event)

        missing_info_streak = 0
        try:
            while not stop_event.wait(DATA_INTERVAL):
                with mt5_lock:
                    info = mt5.account_info()
                if info is None:
                    missing_info_streak += 1
                    if ok:
                        ok = False
                        send_status(sock, "error", "MT5 connection lost")
                    if missing_info_streak >= 3:
                        raise ConnectionError("MT5 account_info unavailable repeatedly")
                    continue

                missing_info_streak = 0
                if not ok:
                    ok = True
                    send_status(sock, "connected", "Reconnected")

                snap = get_snapshot()
                if snap:
                    send_data(sock, snap)
        except KeyboardInterrupt:
            stop_event.set()
            try:
                send_status(sock, "disconnected", "Stopped by user")
            except Exception:
                pass
            break
        except Exception as e:
            print(f"[{instance_id}] loop error: {e}")
        finally:
            stop_event.set()
            try:
                sock.close()
            except Exception:
                pass
            with mt5_lock:
                mt5.shutdown()
            with _slave_origin_lock:
                _slave_origin_positions.clear()
            with _state_lock:
                _last_master_positions.clear()
                _master_driven_close_credit.clear()
                _upstream_close_pending.clear()
                _last_slave_copy_volumes.clear()

        time.sleep(backoff)


if __name__ == "__main__":
    main()
