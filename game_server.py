"""Raw-socket game server. TCP = reliable events, UDP = fast positions. Server-authoritative."""
import json
import os
import random
import socket
import threading
import time

HOST = os.environ.get("GAME_HOST", "127.0.0.1")
TCP_PORT = int(os.environ.get("GAME_TCP_PORT", 9000))
UDP_PORT = int(os.environ.get("GAME_UDP_PORT", 9001))
W, H = 800, 500
TICK_HZ = 20
HIT_DIST = 34  # player-center to coin-center distance that counts as a grab
ANIMALS = {"fox", "wolf", "raccoon", "deer", "penguin", "rabbit", "tortoise", "bear", "panda", "tiger", "lion", "jaguar", "cat"}

lock = threading.Lock()
players = {}          # id -> player dict
coin = {"x": 400, "y": 250}
next_id = 1
udp_sock = None


def enc(msg):
    return (json.dumps(msg, separators=(",", ":"))).encode()


def send_tcp(p, msg):
    try:
        with p["send_lock"]:
            p["conn"].sendall(enc(msg) + b"\n")  # "\n" is our message delimiter (framing)
    except OSError:
        pass


def broadcast_tcp(msg):
    with lock:
        targets = list(players.values())
    for p in targets:
        send_tcp(p, msg)


def lobby_msg():
    with lock:
        return {"t": "lobby", "players": [
            {"id": p["id"], "nick": p["nick"], "score": p["score"], "animal": p["animal"]} for p in players.values()]}


def spawn_coin():
    coin["x"], coin["y"] = random.randint(20, W - 20), random.randint(20, H - 20)


def apply_position(p, x, y):
    """Called for positions arriving on EITHER transport; the server does the collision check."""
    try:
        x, y = float(x), float(y)
    except (TypeError, ValueError):
        return
    x, y = max(0, min(W, x)), max(0, min(H, y))
    hit = None
    with lock:
        p["x"], p["y"] = x, y
        if (x - coin["x"]) ** 2 + (y - coin["y"]) ** 2 <= HIT_DIST ** 2:
            p["score"] += 1
            spawn_coin()
            hit = {"t": "coin_collected", "id": p["id"], "nick": p["nick"], "coin": dict(coin),
                   "scores": {str(q["id"]): q["score"] for q in players.values()}}
    if hit:
        broadcast_tcp(hit)


def register(conn, msg):
    global next_id
    with lock:
        pid, next_id = next_id, next_id + 1
        animal = msg.get("animal") if msg.get("animal") in ANIMALS else "cat"
        p = {"id": pid, "nick": str(msg.get("nick") or f"player{pid}")[:16], "score": 0, "animal": animal,
             "x": W / 2, "y": H / 2, "conn": conn, "send_lock": threading.Lock(),
             "udp_addr": None, "last_seq": 0, "mode": "udp", "udp_out": 0, "tcp_out": 0}
        players[pid] = p
    send_tcp(p, {"t": "welcome", "id": pid, "arena": [W, H], "coin": dict(coin)})
    broadcast_tcp({"t": "joined", "id": pid, "nick": p["nick"]})
    broadcast_tcp(lobby_msg())
    return p


def remove(p):
    with lock:
        players.pop(p["id"], None)
    broadcast_tcp({"t": "left", "id": p["id"], "nick": p["nick"]})
    broadcast_tcp(lobby_msg())


def on_tcp_message(p, msg):
    """Returns False if the client asked to leave."""
    t = msg.get("t")
    if t == "ping":
        send_tcp(p, {"t": "pong", "ts": msg.get("ts")})  # client measures TCP RTT
    elif t == "chat":
        broadcast_tcp({"t": "chat", "id": p["id"], "nick": p["nick"], "text": str(msg.get("text", ""))[:200]})
    elif t == "nick":
        p["nick"] = str(msg.get("nick", p["nick"]))[:16]
        broadcast_tcp(lobby_msg())
    elif t == "animal" and msg.get("animal") in ANIMALS:
        p["animal"] = msg["animal"]
        broadcast_tcp(lobby_msg())
    elif t == "mode" and msg.get("mode") in ("udp", "tcp"):
        p["mode"] = msg["mode"]
    elif t == "pos":  # accepted in any mode; "mode" only picks how the world is sent back
        apply_position(p, msg.get("x"), msg.get("y"))
    elif t == "leave":
        return False
    return True


def handle_client(conn):
    p, buf = None, b""
    try:
        while True:
            chunk = conn.recv(4096)
            if not chunk:
                break
            buf += chunk
            # TCP is a byte stream: one recv may hold half a message or several. Split on "\n".
            while b"\n" in buf:
                line, buf = buf.split(b"\n", 1)
                try:
                    msg = json.loads(line)
                except ValueError:
                    continue
                if p is None:
                    if msg.get("t") == "join":
                        p = register(conn, msg)
                elif not on_tcp_message(p, msg):
                    return
    except OSError:
        pass
    finally:
        if p:
            remove(p)
        conn.close()


def tcp_accept_loop(srv):
    while True:
        conn, _ = srv.accept()
        conn.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        threading.Thread(target=handle_client, args=(conn,), daemon=True).start()


def udp_loop():
    while True:
        data, addr = udp_sock.recvfrom(2048)
        try:
            m = json.loads(data)
        except ValueError:
            continue
        with lock:
            p = players.get(m.get("id"))
        if not p:
            continue
        p["udp_addr"] = addr  # learned from the first datagram (UDP has no connection)
        seq = m.get("seq", 0)
        if seq <= p["last_seq"]:
            continue  # stale or reordered datagram: an old position must not overwrite a newer one
        p["last_seq"] = seq
        if "x" in m:
            apply_position(p, m["x"], m.get("y"))


def tick_loop():
    nxt = time.time()
    while True:
        with lock:
            snap = [{"id": p["id"], "x": round(p["x"], 1), "y": round(p["y"], 1)} for p in players.values()]
            targets = list(players.values())
            c = dict(coin)
        for p in targets:
            msg = {"t": "world", "ts": time.time(), "players": snap, "coin": c}
            if p["mode"] == "udp":
                if p["udp_addr"]:
                    p["udp_out"] += 1  # per-recipient sequence: lets the receiver detect loss/reorder
                    msg["seq"] = p["udp_out"]
                    udp_sock.sendto(enc(msg), p["udp_addr"])
            else:
                p["tcp_out"] += 1
                msg["seq"] = p["tcp_out"]
                send_tcp(p, msg)
        nxt += 1 / TICK_HZ
        time.sleep(max(0, nxt - time.time()))


def start():
    global udp_sock
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind((HOST, TCP_PORT))
    srv.listen()
    udp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    udp_sock.bind((HOST, UDP_PORT))
    spawn_coin()
    for fn, args in ((tcp_accept_loop, (srv,)), (udp_loop, ()), (tick_loop, ())):
        threading.Thread(target=fn, args=args, daemon=True).start()
    print(f"game server: TCP {HOST}:{TCP_PORT}  UDP {HOST}:{UDP_PORT}", flush=True)


if __name__ == "__main__":
    start()
    while True:
        time.sleep(3600)
