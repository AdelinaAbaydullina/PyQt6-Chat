import json
import socketserver
import struct
import threading


HOST = "127.0.0.1"
PORT = 50505


def recv_exact(sock, size):
    chunks = []
    received = 0
    while received < size:
        chunk = sock.recv(size - received)
        if not chunk:
            return None
        chunks.append(chunk)
        received += len(chunk)
    return b"".join(chunks)


def recv_message(sock):
    header = recv_exact(sock, 4)
    if header is None:
        return None

    payload_size = struct.unpack("!I", header)[0]
    payload = recv_exact(sock, payload_size)
    if payload is None:
        return None

    return json.loads(payload.decode("utf-8"))


def send_message(sock, message):
    payload = json.dumps(message, ensure_ascii=False).encode("utf-8")
    header = struct.pack("!I", len(payload))
    sock.sendall(header + payload)


class ChatHandler(socketserver.BaseRequestHandler):
    clients = set()
    clients_lock = threading.Lock()

    def handle(self):
        client = self.request
        with self.clients_lock:
            self.clients.add(client)
        print(f"Client connected: {self.client_address}")

        try:
            while True:
                message = recv_message(client)
                if message is None:
                    break
                print("Received:", message)
                self.broadcast(message)
        except (ConnectionError, OSError, json.JSONDecodeError) as exc:
            print(f"Client error: {exc}")
        finally:
            with self.clients_lock:
                self.clients.discard(client)
            try:
                client.close()
            except OSError:
                pass
            print(f"Client disconnected: {self.client_address}")

    @classmethod
    def broadcast(cls, message):
        with cls.clients_lock:
            clients = list(cls.clients)

        dead_clients = []
        for client in clients:
            try:
                send_message(client, message)
            except OSError:
                dead_clients.append(client)

        if dead_clients:
            with cls.clients_lock:
                for client in dead_clients:
                    cls.clients.discard(client)


class ThreadedChatServer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True


if __name__ == "__main__":
    with ThreadedChatServer((HOST, PORT), ChatHandler) as server:
        print(f"Chat server is running on {HOST}:{PORT}")
        print("Start main.py in one or more windows. Stop with Ctrl+C.")
        server.serve_forever()