import json
import socket
import struct
import sys
from queue import Queue

from PyQt6.QtCore import QThread, pyqtSignal
from PyQt6.QtWidgets import (
    QApplication,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QMainWindow,
    QPushButton,
    QTextEdit,
    QVBoxLayout,
    QWidget,
)

HOST = "127.0.0.1"
PORT = 50505


class NetCom:
    def __init__(self, sock):
        self.sock = sock

    @staticmethod
    def _recv_exact(sock, size):
        chunks = []
        received = 0

        while received < size:
            chunk = sock.recv(size - received)
            if not chunk:
                return None

            chunks.append(chunk)
            received += len(chunk)

        return b"".join(chunks)

    def recv(self):
        header = self._recv_exact(self.sock, 4)
        if header is None:
            return None

        payload_size = struct.unpack("!I", header)[0]
        payload = self._recv_exact(self.sock, payload_size)
        if payload is None:
            return None

        return json.loads(payload.decode("utf-8"))

    def send(self, message):
        payload = json.dumps(
            message, ensure_ascii=False
        ).encode("utf-8")
        header = struct.pack("!I", len(payload))
        self.sock.sendall(header + payload)


class ReceiveThread(QThread):
    message_received = pyqtSignal(dict)
    connection_lost = pyqtSignal(str)

    def __init__(self, net):
        super().__init__()
        self.net = net

    def run(self):
        while True:
            try:
                message = self.net.recv()

                # recv() возвращает None, если сервер закрыл соединение.
                if message is None:
                    self.connection_lost.emit(
                        "Сервер закрыл соединение."
                    )
                    break

                self.message_received.emit(message)

            except (OSError, ConnectionError) as exc:
                self.connection_lost.emit(
                    f"Потеряно соединение с сервером: {exc}"
                )
                break
            except (json.JSONDecodeError, UnicodeDecodeError,
                    struct.error, ValueError) as exc:
                self.connection_lost.emit(
                    f"Ошибка при чтении данных: {exc}"
                )
                break


class SendThread(QThread):
    connection_lost = pyqtSignal(str)

    def __init__(self, net):
        super().__init__()
        self.net = net
        self.queue = Queue()

    def send(self, message):
        self.queue.put(message)

    def stop(self):
        # None будит поток, ожидающий queue.get().
        self.queue.put(None)

    def run(self):
        while True:
            message = self.queue.get()

            if message is None:
                break

            try:
                self.net.send(message)
            except (OSError, ConnectionError) as exc:
                self.connection_lost.emit(
                    f"Не удалось отправить сообщение: {exc}"
                )
                break


class MainWindow(QMainWindow):
    def __init__(self, net):
        super().__init__()
        self.net = net
        self._closing = False
        self._disconnected = False

        self.setWindowTitle("PyQt6 network chat")
        self.resize(560, 420)

        self.status_label = QLabel("Connected")
        self.username_input = QLineEdit()
        self.username_input.setPlaceholderText("Введите ваше имя")

        self.history = QTextEdit()
        self.history.setReadOnly(True)

        self.input = QLineEdit()
        self.input.setPlaceholderText("Введите сообщение...")

        self.send_button = QPushButton("Send")

        layout = QVBoxLayout()
        layout.addWidget(self.status_label)
        layout.addWidget(QLabel("Имя пользователя:"))
        layout.addWidget(self.username_input)
        layout.addWidget(self.history)

        input_row = QHBoxLayout()
        input_row.addWidget(self.input)
        input_row.addWidget(self.send_button)
        layout.addLayout(input_row)

        root = QWidget()
        root.setLayout(layout)
        self.setCentralWidget(root)

        self.send_button.clicked.connect(self.on_send_clicked)
        self.input.returnPressed.connect(self.on_send_clicked)

        self.receive_thread = ReceiveThread(self.net)
        self.receive_thread.message_received.connect(
            self.handle_message
        )
        self.receive_thread.connection_lost.connect(
            self.handle_connection_lost
        )
        self.receive_thread.start()

        self.send_thread = SendThread(self.net)
        self.send_thread.connection_lost.connect(
            self.handle_connection_lost
        )
        self.send_thread.start()

        self.history.append("[Система] Подключение установлено")

    def on_send_clicked(self):
        if self._disconnected or self._closing:
            return

        username = self.username_input.text().strip()
        text = self.input.text().strip()

        if not username:
            self.history.append(
                "[Система] Сначала введите имя пользователя."
            )
            return

        if not text:
            return

        self.send_thread.send({
            "type": "chat",
            "username": username,
            "text": text,
        })
        self.input.clear()

    def handle_connection_lost(self, error):
        if self._closing or self._disconnected:
            return

        self._disconnected = True
        self.status_label.setText("Disconnected")
        self.history.append(f"[Система] {error}")

        self.input.setEnabled(False)
        self.send_button.setEnabled(False)

    def handle_message(self, message):
        message_type = message.get("type")

        if message_type == "chat":
            username = message.get(
                "username", "Неизвестный пользователь"
            )
            text = message.get("text", "")
            self.history.append(f"{username}: {text}")

        elif message_type == "system":
            self.history.append(
                f"[Система] {message.get('text', '')}"
            )

        else:
            print("Неизвестный тип сообщения:", message_type)

    def closeEvent(self, event):
        self._closing = True

        # Сначала организуем остановку SendThread.
        self.send_thread.stop()

        # shutdown() разблокирует recv()/sendall(), если они ждут сокет.
        try:
            self.net.sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass

        # Оба потока уже получили условия для завершения.
        self.receive_thread.wait()
        self.send_thread.wait()

        try:
            self.net.sock.close()
        except OSError:
            pass

        event.accept()


def main():
    app = QApplication(sys.argv)

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    try:
        sock.connect((HOST, PORT))
    except OSError as exc:
        print(f"Не удалось подключиться к {HOST}:{PORT}: {exc}")
        print("Сначала запустите server.py, затем main.py.")
        sock.close()
        return

    net = NetCom(sock)
    window = MainWindow(net)
    window.show()

    exit_code = app.exec()
    raise SystemExit(exit_code)


if __name__ == "__main__":
    main()

