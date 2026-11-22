"""Terminal test client (raw sockets). Joins, chases the coin, prints TCP events and UDP stats."""
import argparse
import json
import random
import socket
import threading
import time

ap = argparse.ArgumentParser()
ap.add_argument("--nick", default=f"bot{random.randint(1, 99)}")
ap.add_argument("--mode", choices=["udp", "tcp"], default="udp")
ap.add_argument("--host", default="127.0.0.1")
ap.add_argument("--tcp", type=int, default=9000)
ap.add_argument("--udp", type=int, default=9001)
a = ap.parse_args()

tcp = socket.create_connection((a.host, a.tcp))
udp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
tcp_lock = threading.Lock()
got_id = threading.Event()
me = {"id": None, "x": 400.0, "y": 250.0}
world = {"coin": None, "players": []}
st = {"rx": 0, "lost": 0, "ooo": 0, "last": 0}  # UDP world stats, from sequence numbers


def send_tcp(m):
    with tcp_lock:
        tcp.sendall(json.dumps(m).encode() + b"\n")


def on_world(m, src):
    world.update(coin=m["coin"], players=m["players"])
    if src == "UDP":
        st["rx"] += 1
        seq = m["seq"]
        if seq > st["last"]:
            st["lost"] += max(0, seq - st["last"] - 1) if st["last"] else 0
            st["last"] = seq
        else:  # arrived after a newer one: out of order, and not actually lost
            st["ooo"] += 1
            st["lost"] = max(0, st["lost"] - 1)


def tcp_reader():
    buf = b""
    while True:
        chunk = tcp.recv(4096)
        if not chunk:
            print("[TCP] server closed", flush=True)
            return
        buf += chunk
        while b"\n" in buf:  # framing: partial/merged reads
            line, buf = buf.split(b"\n", 1)
            m = json.loads(line)
            t = m["t"]
            if t == "world":
                on_world(m, "TCP")
            elif t == "welcome":
                me["id"] = m["id"]
                got_id.set()
                print(f"[TCP] welcome id={m['id']} coin={m['coin']}", flush=True)
            elif t == "pong":
                print(f"[TCP] pong RTT={(time.time() - m['ts']) * 1000:.1f} ms", flush=True)
            else:
                print(f"[TCP] {t}: {json.dumps(m)}", flush=True)


def udp_reader():
    while True:
        data, _ = udp.recvfrom(2048)
        on_world(json.loads(data), "UDP")


threading.Thread(target=tcp_reader, daemon=True).start()
send_tcp({"t": "join", "nick": a.nick})
got_id.wait()
send_tcp({"t": "mode", "mode": a.mode})
udp.sendto(json.dumps({"id": me["id"], "seq": 0}).encode(), (a.host, a.udp))  # hello: server learns our UDP addr
threading.Thread(target=udp_reader, daemon=True).start()
send_tcp({"t": "chat", "text": f"hi from {a.nick}"})

seq, last_print, last_ping = 0, time.time(), 0
while True:
    c = world["coin"]
    if c:  # walk toward the coin, 6 px per axis per tick
        me["x"] += max(-6, min(6, c["x"] - me["x"]))
        me["y"] += max(-6, min(6, c["y"] - me["y"]))
    seq += 1
    if a.mode == "udp":
        udp.sendto(json.dumps({"id": me["id"], "seq": seq, "ts": time.time(), "x": me["x"], "y": me["y"]}).encode(),
                   (a.host, a.udp))
    else:
        send_tcp({"t": "pos", "x": me["x"], "y": me["y"]})
    now = time.time()
    if now - last_ping > 2:
        send_tcp({"t": "ping", "ts": now})
        last_ping = now
    if now - last_print >= 1:
        pos = " ".join(f"{p['id']}:({p['x']:.0f},{p['y']:.0f})" for p in world["players"])
        print(f"[UDP] rx/s={st['rx']} lost={st['lost']} ooo={st['ooo']} | {pos}", flush=True)
        st["rx"], last_print = 0, now
    time.sleep(0.05)
