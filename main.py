"""FastAPI app: serves index.html and bridges each browser WebSocket to its own raw TCP + UDP sockets."""
import asyncio
import json
import random
import socket
import time
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse

import game_server

GAME_HOST = "127.0.0.1"  # game server runs in this same process, reached over loopback


@asynccontextmanager
async def lifespan(app):
    game_server.start()  # raw TCP + UDP servers in background threads
    yield


app = FastAPI(lifespan=lifespan)


@app.get("/")
def index():
    return FileResponse(Path(__file__).with_name("index.html"))


class Bridge:
    """One per browser player. Browsers cannot open raw sockets, so this bridge opens a TCP and a UDP socket for them."""

    def __init__(self, ws):
        self.ws, self.loop = ws, asyncio.get_running_loop()
        self.ws_lock = asyncio.Lock()
        self.id, self.mode, self.seq = None, "udp", 0
        # SIMULATED impairment, set live from the UI. Never real network loss.
        self.loss, self.udp_delay, self.tcp_delay = 0, 0, 0
        self.tcp_in, self.tcp_out = asyncio.Queue(), asyncio.Queue()
        # Stats, computed from real packets/sequence numbers
        self.tcp_sent = self.udp_sent = self.udp_rx = self.ooo = 0
        self.base = self.max_seq = None
        self.rtt = None

    async def connect(self):
        self.tcp = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.tcp.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.tcp.setblocking(False)
        await self.loop.sock_connect(self.tcp, (GAME_HOST, game_server.TCP_PORT))
        self.udp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.udp.setblocking(False)
        self.udp.connect((GAME_HOST, game_server.UDP_PORT))

    def close(self):
        self.tcp.close()
        self.udp.close()

    async def ws_send(self, msg):
        try:
            async with self.ws_lock:
                await self.ws.send_text(json.dumps(msg))
        except Exception:
            pass

    # ---------- TCP path (ordered; delay is applied in order, so it models head-of-line blocking) ----------
    def tcp_send(self, msg):
        self.tcp_sent += 1
        due = time.monotonic() + self.tcp_delay / 1000  # SIMULATED added delay
        self.tcp_out.put_nowait((due, json.dumps(msg).encode() + b"\n"))

    async def tcp_out_worker(self):
        while True:
            due, data = await self.tcp_out.get()
            await asyncio.sleep(max(0, due - time.monotonic()))
            await self.loop.sock_sendall(self.tcp, data)

    async def tcp_reader(self):
        buf = b""
        while True:
            chunk = await self.loop.sock_recv(self.tcp, 4096)
            if not chunk:
                return
            buf += chunk
            while b"\n" in buf:  # framing: one recv can hold partial or several messages
                line, buf = buf.split(b"\n", 1)
                due = time.monotonic() + self.tcp_delay / 1000  # SIMULATED added delay
                self.tcp_in.put_nowait((due, json.loads(line)))

    async def tcp_in_worker(self):
        while True:
            due, m = await self.tcp_in.get()
            await asyncio.sleep(max(0, due - time.monotonic()))
            if m["t"] == "pong":  # RTT includes both simulated TCP delays
                self.rtt = (time.monotonic() - m["ts"]) * 1000
                continue
            if m["t"] == "welcome":
                self.id = m["id"]
                self.udp_raw(json.dumps({"id": self.id, "seq": 0}).encode())  # hello: server learns our UDP address
            m["via"] = "tcp"
            await self.ws_send(m)

    # ---------- UDP path (unordered; each datagram is delayed/dropped independently) ----------
    def udp_raw(self, data):
        try:
            self.udp.send(data)
        except OSError:
            pass

    def send_pos(self, x, y):
        if self.id is None:
            return
        self.seq += 1
        if self.mode == "tcp":
            self.tcp_send({"t": "pos", "x": x, "y": y})
            return
        self.udp_sent += 1
        if random.random() * 100 < self.loss:  # SIMULATED loss
            return
        data = json.dumps({"id": self.id, "seq": self.seq, "ts": time.time(), "x": x, "y": y}).encode()
        if self.udp_delay:  # SIMULATED added delay
            self.loop.call_later(self.udp_delay / 1000, self.udp_raw, data)
        else:
            self.udp_raw(data)

    async def udp_reader(self):
        while True:
            data = await self.loop.sock_recv(self.udp, 2048)
            if random.random() * 100 < self.loss:  # SIMULATED loss
                continue
            try:
                m = json.loads(data)
            except ValueError:
                continue
            if self.udp_delay:  # SIMULATED added delay
                self.loop.call_later(self.udp_delay / 1000, self.deliver_udp, m)
            else:
                self.deliver_udp(m)

    def deliver_udp(self, m):
        seq = m["seq"]
        if self.base is None:
            self.base = self.max_seq = seq - 1
        self.udp_rx += 1
        if seq < self.max_seq:
            self.ooo += 1  # arrived after a newer datagram
        else:
            self.max_seq = seq
        m["via"] = "udp"
        asyncio.ensure_future(self.ws_send(m))

    # ---------- stats, once per second ----------
    async def stats_loop(self):
        while True:
            await asyncio.sleep(1)
            self.tcp_send({"t": "ping", "ts": time.monotonic()})
            expected = self.max_seq - self.base if self.base is not None else 0
            lost = max(0, expected - self.udp_rx)  # gaps in the sequence numbers
            await self.ws_send({
                "t": "stats", "mode": self.mode, "udp_pps": self.udp_rx,
                "loss_pct": round(100 * lost / expected, 1) if expected else 0,
                "lost": lost, "ooo": self.ooo,
                "rtt_ms": None if self.rtt is None else round(self.rtt, 1),
                "tcp_sent": self.tcp_sent, "udp_sent": self.udp_sent})
            if self.base is not None:
                self.base, self.udp_rx = self.max_seq, 0

    # ---------- messages from the browser ----------
    def from_browser(self, m):
        t = m.get("t")
        if t == "join":
            self.tcp_send({"t": "join", "nick": str(m.get("nick", ""))[:16], "animal": str(m.get("animal", ""))[:12]})
        elif t == "animal":
            self.tcp_send({"t": "animal", "animal": str(m.get("animal", ""))[:12]})
        elif t == "nick":
            self.tcp_send({"t": "nick", "nick": str(m.get("nick", ""))[:16]})
        elif t == "chat":
            self.tcp_send({"t": "chat", "text": str(m.get("text", ""))[:200]})
        elif t == "pos":
            self.send_pos(float(m.get("x", 0)), float(m.get("y", 0)))
        elif t == "mode" and m.get("mode") in ("udp", "tcp"):
            self.mode, self.base = m["mode"], None  # reset loss window: stream restarts
            self.tcp_send({"t": "mode", "mode": self.mode})
        elif t == "impair":  # SIMULATED
            clamp = lambda v, hi: max(0, min(hi, float(v)))
            self.loss = clamp(m.get("loss", 0), 50)
            self.udp_delay = clamp(m.get("udp_delay", 0), 500)
            self.tcp_delay = clamp(m.get("tcp_delay", 0), 500)


@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket):
    await ws.accept()
    b = Bridge(ws)
    await b.connect()
    tasks = [asyncio.create_task(c) for c in
             (b.tcp_reader(), b.tcp_in_worker(), b.tcp_out_worker(), b.udp_reader(), b.stats_loop())]
    try:
        while True:
            b.from_browser(json.loads(await ws.receive_text()))
    except (WebSocketDisconnect, ValueError):
        pass
    finally:
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)  # let the loop unregister the sockets first
        b.close()  # server sees TCP EOF and removes the player
