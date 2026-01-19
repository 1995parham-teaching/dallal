#!/usr/bin/env python3
"""
Message Broker CLI Client - Publish and subscribe to topics.

Usage:
    python3 client.py <host> <port> publish <topic> <message>
    python3 client.py <host> <port> subscribe <topic1> [topic2 ...]
"""

import argparse
import json
import socket
import sys

ACK_TIMEOUT = 10  # seconds


def send_message(sock: socket.socket, msg: dict) -> None:
    """Send a JSON message to the server."""
    data = json.dumps(msg) + "\n"
    sock.sendall(data.encode("utf-8"))


def receive_message(sock: socket.socket, timeout: float | None = None) -> dict | None:
    """Receive a JSON message from the server."""
    original_timeout = sock.gettimeout()
    if timeout is not None:
        sock.settimeout(timeout)

    buffer = ""
    try:
        while "\n" not in buffer:
            data = sock.recv(4096)
            if not data:
                return None
            buffer += data.decode("utf-8")
        line, _ = buffer.split("\n", 1)
        return json.loads(line)
    except socket.timeout:
        print("Timeout waiting for server response")
        return None
    except json.JSONDecodeError:
        print(f"Invalid JSON received: {buffer}")
        return None
    finally:
        sock.settimeout(original_timeout)


def cmd_publish(host: str, port: int, topic: str, message: str) -> None:
    """Publish a message to a topic."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.connect((host, port))
        send_message(sock, {"type": "publish", "topic": topic, "message": message})

        response = receive_message(sock, timeout=ACK_TIMEOUT)
        if response and response.get("type") == "puback":
            print(f"Message published to topic '{topic}'")
        else:
            print("Failed to receive publish acknowledgment")
            sys.exit(1)
    except ConnectionRefusedError:
        print(f"Could not connect to {host}:{port}")
        sys.exit(1)
    finally:
        sock.close()


def cmd_subscribe(host: str, port: int, topics: list[str]) -> None:
    """Subscribe to topics and receive messages."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.connect((host, port))

        # Subscribe to all topics
        for topic in topics:
            send_message(sock, {"type": "subscribe", "topic": topic})
            response = receive_message(sock, timeout=ACK_TIMEOUT)
            if response and response.get("type") == "suback":
                print(f"Subscribed to topic '{topic}'")
            else:
                print(f"Failed to subscribe to topic '{topic}'")
                sys.exit(1)

        print("Waiting for messages... (Ctrl+C to exit)")

        # Receive messages loop
        buffer = ""
        while True:
            data = sock.recv(4096)
            if not data:
                print("Server disconnected")
                break
            buffer += data.decode("utf-8")
            while "\n" in buffer:
                line, buffer = buffer.split("\n", 1)
                if line.strip():
                    try:
                        message = json.loads(line)
                        handle_message(sock, message)
                    except json.JSONDecodeError:
                        print(f"Invalid JSON received: {line}")

    except ConnectionRefusedError:
        print(f"Could not connect to {host}:{port}")
        sys.exit(1)
    except KeyboardInterrupt:
        print("\nDisconnecting...")
    finally:
        sock.close()


def handle_message(sock: socket.socket, message: dict) -> None:
    """Handle an incoming message from the server."""
    msg_type = message.get("type")

    if msg_type == "message":
        topic = message.get("topic")
        content = message.get("message")
        print(f"[{topic}] {content}")

    elif msg_type == "ping":
        send_message(sock, {"type": "pong"})


def main():
    parser = argparse.ArgumentParser(description="Message Broker CLI Client")
    parser.add_argument("host", help="Server host")
    parser.add_argument("port", type=int, help="Server port")

    subparsers = parser.add_subparsers(dest="command", required=True)

    # Publish command
    pub_parser = subparsers.add_parser("publish", help="Publish a message")
    pub_parser.add_argument("topic", help="Topic to publish to")
    pub_parser.add_argument("message", help="Message to publish")

    # Subscribe command
    sub_parser = subparsers.add_parser("subscribe", help="Subscribe to topics")
    sub_parser.add_argument("topics", nargs="+", help="Topics to subscribe to")

    args = parser.parse_args()

    if args.command == "publish":
        cmd_publish(args.host, args.port, args.topic, args.message)
    elif args.command == "subscribe":
        cmd_subscribe(args.host, args.port, args.topics)


if __name__ == "__main__":
    main()
