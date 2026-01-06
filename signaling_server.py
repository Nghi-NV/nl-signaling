#!/usr/bin/env python3
"""
Simple WebSocket signaling server for WebRTC peer connection.

Usage: python signaling_server.py [port]
Default port: 8080
"""

import asyncio
import json
import sys
from typing import Dict, Set
from dataclasses import dataclass, field

try:
    import websockets
except ImportError:
    print("Installing websockets...")
    import subprocess
    subprocess.check_call([sys.executable, "-m", "pip", "install", "websockets"])
    import websockets


@dataclass
class Room:
    """Represents a signaling room"""
    host: websockets.WebSocketServerProtocol | None = None
    viewers: Set[websockets.WebSocketServerProtocol] = field(default_factory=set)


# Global rooms storage
rooms: Dict[str, Room] = {}


async def handle_client(websocket: websockets.WebSocketServerProtocol):
    """Handle a single WebSocket client"""
    room_id = None
    is_host = False
    
    try:
        async for message in websocket:
            try:
                data = json.loads(message)
                msg_type = data.get("type")
                msg_data = data.get("data", {})
                
                if msg_type == "Join":
                    room_id = msg_data.get("room_id")
                    role = msg_data.get("role")
                    is_host = role == "Host"
                    
                    if room_id not in rooms:
                        rooms[room_id] = Room()
                    
                    room = rooms[room_id]
                    
                    if is_host:
                        room.host = websocket
                        print(f"[Room {room_id}] Host joined")
                        
                        # Send room created confirmation
                        await websocket.send(json.dumps({
                            "type": "RoomCreated",
                            "data": {"room_id": room_id}
                        }))
                    else:
                        room.viewers.add(websocket)
                        print(f"[Room {room_id}] Viewer joined ({len(room.viewers)} viewers)")
                        
                        # Notify host about new viewer
                        if room.host:
                            await room.host.send(json.dumps({
                                "type": "PeerJoined",
                                "data": {"peer_id": str(id(websocket))}
                            }))
                
                elif msg_type == "Offer":
                    # Forward offer from host to all viewers
                    if room_id and room_id in rooms:
                        room = rooms[room_id]
                        for viewer in room.viewers:
                            await viewer.send(json.dumps({
                                "type": "Offer",
                                "data": {"sdp": msg_data.get("sdp")}
                            }))
                        print(f"[Room {room_id}] Offer forwarded to {len(room.viewers)} viewers")
                
                elif msg_type == "Answer":
                    # Forward answer from viewer to host
                    if room_id and room_id in rooms:
                        room = rooms[room_id]
                        if room.host:
                            await room.host.send(json.dumps({
                                "type": "Answer",
                                "data": {"sdp": msg_data.get("sdp")}
                            }))
                        print(f"[Room {room_id}] Answer forwarded to host")
                
                elif msg_type == "IceCandidate":
                    # Forward ICE candidate to other peer(s)
                    if room_id and room_id in rooms:
                        room = rooms[room_id]
                        if is_host:
                            for viewer in room.viewers:
                                await viewer.send(json.dumps({
                                    "type": "IceCandidate",
                                    "data": {"candidate": msg_data.get("candidate")}
                                }))
                        elif room.host:
                            await room.host.send(json.dumps({
                                "type": "IceCandidate",
                                "data": {"candidate": msg_data.get("candidate")}
                            }))
                
            except json.JSONDecodeError:
                print(f"Invalid JSON: {message}")
    
    except websockets.exceptions.ConnectionClosed:
        pass
    
    finally:
        # Cleanup on disconnect
        if room_id and room_id in rooms:
            room = rooms[room_id]
            if is_host:
                room.host = None
                print(f"[Room {room_id}] Host left")
                # Notify viewers
                for viewer in room.viewers:
                    try:
                        await viewer.send(json.dumps({
                            "type": "PeerLeft",
                            "data": {"peer_id": "host"}
                        }))
                    except:
                        pass
            else:
                room.viewers.discard(websocket)
                print(f"[Room {room_id}] Viewer left ({len(room.viewers)} remaining)")
                # Notify host
                if room.host:
                    try:
                        await room.host.send(json.dumps({
                            "type": "PeerLeft",
                            "data": {"peer_id": str(id(websocket))}
                        }))
                    except:
                        pass
            
            # Clean up empty rooms
            if room.host is None and len(room.viewers) == 0:
                del rooms[room_id]
                print(f"[Room {room_id}] Room deleted (empty)")


async def main(port: int = 8080):
    """Start the signaling server"""
    print(f"Starting signaling server on ws://0.0.0.0:{port}")
    print("Waiting for connections...")
    
    async def health_check(connection, request):
        # Allow WebSocket upgrades to pass through
        if "Upgrade" in request.headers and request.headers["Upgrade"].lower() == "websocket":
            return None
            
        # Intercept HTTP health checks on / and /health
        if request.path == "/health" or request.path == "/":
            return connection.respond(200, "OK")
    
    async with websockets.serve(handle_client, "0.0.0.0", port, process_request=health_check):
        await asyncio.Future()  # Run forever


if __name__ == "__main__":
    import os
    # Priority: Command line arg > PORT env var > Default 8080
    if len(sys.argv) > 1:
        port = int(sys.argv[1])
    else:
        port = int(os.environ.get("PORT", 8080))
    
    asyncio.run(main(port))
