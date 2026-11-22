# TCP vs UDP Coin Grab

Multiplayer arena built on raw Python sockets. One process: a raw TCP + UDP game server (`game_server.py`)
and a FastAPI app (`main.py`) that bridges each browser WebSocket to its own TCP and UDP sockets
(browsers cannot open raw sockets).

## Run locally
    pip install -r requirements.txt
    uvicorn main:app --port 8000
Open http://localhost:8000 in two tabs. Terminal-only test (server alone, then two clients):

    python game_server.py
    python test_client.py --nick alice
    python test_client.py --nick bob --mode tcp

## Deploy (Railway)
Push this folder to a GitHub repo, create a Railway service from it. `requirements.txt`, `Procfile`
(`uvicorn main:app --host 0.0.0.0 --port $PORT`) and `runtime.txt` (Python 3.11) are all it needs.
Railway only exposes the web port; the game sockets (9000 TCP / 9001 UDP, override with `GAME_TCP_PORT`
/ `GAME_UDP_PORT`) stay internal on 127.0.0.1.

## What each protocol carries, and why
- **TCP**: join/leave, nickname, chat, coin_collected + scores, lobby list, ping/pong. These must arrive,
  in order, exactly once. Newline-delimited JSON, with a buffer that handles partial and merged reads.
- **UDP**: player positions and the 20 Hz world snapshot. A lost position is replaced by the next one
  50 ms later, so retransmitting it would only add lag. Each datagram has a sequence number and timestamp;
  the server drops stale ones.
- Position mode can be switched live: "tcp" mode sends positions and receives the world over the TCP stream.

## Loopback-UDP note
Bridge and game server talk over 127.0.0.1, where UDP almost never drops or reorders packets.
So the loss you see in the UI comes from the simulator, not from the network.

## Simulated vs real
- **Simulated**: UDP loss %, UDP delay, TCP delay. They are applied in the bridge, per player, on both directions.
  TCP delay is applied in order (head-of-line blocking); TCP loss is not simulated because TCP would retransmit it.
- **Real**: sequence numbers, loss % (from sequence gaps), out-of-order count, packets/s, TCP RTT (ping/pong,
  so it includes the simulated TCP delay both ways), events sent per protocol, message framing, collision checks.
- The browser-to-bridge WebSocket leg runs over TCP, so it is always reliable.
