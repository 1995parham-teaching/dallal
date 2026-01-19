#!/usr/bin/env python3
"""
Message Broker Server - TCP-based publish/subscribe message broker.

Usage:
    python3 server.py [--host HOST] [--port PORT]
"""

import argparse
import json
import socket
import threading


class MessageBroker:
    """Thread-safe message broker handling multiple client connections."""

    def __init__(self):
        self.clients: dict[socket.socket, set[str]] = {}
        self.lock = threading.Lock()

    def start(self, host: str, port: int) -> None:
        """Start the message broker server."""
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_socket.bind((host, port))
        server_socket.listen(5)
        print(f"Message broker listening on {host}:{port}")

        try:
            while True:
                conn, addr = server_socket.accept()
                print(f"New connection from {addr}")
                with self.lock:
                    self.clients[conn] = set()
                client_thread = threading.Thread(
                    target=self.handle_client, args=(conn, addr), daemon=True
                )
                client_thread.start()
        except KeyboardInterrupt:
            print("\nShutting down server...")
        finally:
            server_socket.close()

    def handle_client(self, conn: socket.socket, addr: tuple) -> None:
        """Handle a single client connection."""
        buffer = ""
        try:
            while True:
                data = conn.recv(4096)
                if not data:
                    break
                buffer += data.decode("utf-8")
                while "\n" in buffer:
                    line, buffer = buffer.split("\n", 1)
                    if line.strip():
                        try:
                            message = json.loads(line)
                            self.process_message(conn, message)
                        except json.JSONDecodeError:
                            print(f"Invalid JSON from {addr}: {line}")
        except (ConnectionResetError, BrokenPipeError):
            pass
        finally:
            print(f"Client {addr} disconnected")
            self.remove_client(conn)

    def process_message(self, conn: socket.socket, message: dict) -> None:
        """Process a message from a client."""
        msg_type = message.get("type")

        if msg_type == "subscribe":
            topic = message.get("topic")
            if topic:
                with self.lock:
                    if conn in self.clients:
                        self.clients[conn].add(topic)
                self.send_message(conn, {"type": "suback", "topic": topic})
                print(f"Client subscribed to topic: {topic}")

        elif msg_type == "publish":
            topic = message.get("topic")
            msg_content = message.get("message")
            if topic and msg_content is not None:
                self.send_message(conn, {"type": "puback"})
                self.broadcast(topic, msg_content)
                print(f"Published to topic '{topic}': {msg_content}")

        elif msg_type == "ping":
            self.send_message(conn, {"type": "pong"})

        elif msg_type == "pong":
            pass  # Response to our ping, nothing to do

    def broadcast(self, topic: str, message: str) -> None:
        """Broadcast a message to all subscribers of a topic."""
        with self.lock:
            subscribers = [
                conn for conn, topics in self.clients.items() if topic in topics
            ]

        msg = {"type": "message", "topic": topic, "message": message}
        for conn in subscribers:
            self.send_message(conn, msg)

    def send_message(self, conn: socket.socket, message: dict) -> None:
        """Send a JSON message to a client."""
        try:
            data = json.dumps(message) + "\n"
            conn.sendall(data.encode("utf-8"))
        except (ConnectionResetError, BrokenPipeError, OSError):
            self.remove_client(conn)

    def remove_client(self, conn: socket.socket) -> None:
        """Remove a client from the broker."""
        with self.lock:
            if conn in self.clients:
                del self.clients[conn]
        try:
            conn.close()
        except OSError:
            pass


def main():
    parser = argparse.ArgumentParser(description="Message Broker Server")
    parser.add_argument("--host", default="0.0.0.0", help="Host to bind to")
    parser.add_argument("--port", type=int, default=1373, help="Port to listen on")
    args = parser.parse_args()

    broker = MessageBroker()
    broker.start(args.host, args.port)


if __name__ == "__main__":
    main()
