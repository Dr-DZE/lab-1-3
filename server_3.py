import socket
import os
import time
import json
import struct
from collections import defaultdict
from threading import Thread


class FileServer:
    def __init__(self):
        self.tcp_port = 12345
        self.udp_port = 12346
        self.active_transfers = defaultdict(dict)
        self.partial_dir = "partial_transfers"
        self.setup_dirs()

    def setup_dirs(self):
        os.makedirs(self.partial_dir, exist_ok=True)

    def start(self):
        # Запуск TCP сервера
        tcp_thread = Thread(target=self.start_tcp_server, daemon=True)
        tcp_thread.start()

        # Запуск UDP сервера
        udp_thread = Thread(target=self.start_udp_server, daemon=True)
        udp_thread.start()

        print(f"Server started. TCP:{self.tcp_port}, UDP:{self.udp_port}")
        tcp_thread.join()
        udp_thread.join()

    def start_tcp_server(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.bind(("0.0.0.0", self.tcp_port))
            s.listen()

            while True:
                conn, addr = s.accept()
                Thread(target=self.handle_tcp_client, args=(conn, addr)).start()

    def start_udp_server(self):
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            s.bind(("0.0.0.0", self.udp_port))

            while True:
                data, addr = s.recvfrom(65536)
                Thread(target=self.handle_udp_packet, args=(s, data, addr)).start()

    def handle_tcp_client(self, conn, addr):
        try:
            client_id = f"{addr[0]}:{addr[1]}"
            conn.settimeout(30)

            while True:
                try:
                    cmd = conn.recv(1024).decode().strip()
                    if not cmd:
                        break

                    if cmd.upper() == "TIME":
                        conn.sendall(time.strftime("%Y-%m-%d %H:%M:%S").encode())
                    elif cmd.upper().startswith("UPLOAD"):
                        self.handle_tcp_upload(conn, client_id, cmd)
                    elif cmd.upper().startswith("DOWNLOAD"):
                        self.handle_tcp_download(conn, client_id, cmd)
                    else:
                        conn.sendall(b"Unknown command")
                except socket.timeout:
                    continue
                except Exception as e:
                    print(f"Error processing command: {e}")
                    break

        except Exception as e:
            print(f"TCP Error with {addr}: {e}")
        finally:
            conn.close()

    def handle_udp_packet(self, sock, data, addr):
        try:
            if len(data) < 6:
                return

            header = data[:6]
            payload = data[6:]
            ptype, flags, seq, ack = struct.unpack("!BBHH", header)
            client_id = f"{addr[0]}:{addr[1]}"

            if ptype == 1:  # Data packet
                self.handle_udp_data(sock, client_id, seq, payload, addr)
            elif ptype == 2:  # ACK
                pass  # Клиенты не отправляют ACK серверу
            elif ptype == 4:  # File info
                self.handle_udp_file_info(sock, client_id, payload, addr)
            elif ptype == 5:  # Finish
                self.complete_udp_transfer(client_id)
        except Exception as e:
            print(f"UDP processing error: {e}")

    def handle_tcp_upload(self, conn, client_id, cmd):
        try:
            parts = cmd.split()
            if len(parts) < 3:
                conn.sendall(b"Invalid UPLOAD command")
                return

            filename = parts[1]
            filesize = int(parts[2])
            temp_path = os.path.join(self.partial_dir, f"{client_id}_{filename}")

            received = 0
            with open(temp_path, "wb") as f:
                while received < filesize:
                    data = conn.recv(min(4096, filesize - received))
                    if not data:
                        break
                    f.write(data)
                    received += len(data)

            if received == filesize:
                os.replace(temp_path, filename)
                conn.sendall(b"UPLOAD COMPLETE")
            else:
                conn.sendall(f"UPLOAD PARTIAL {received}/{filesize}".encode())
        except Exception as e:
            conn.sendall(f"UPLOAD ERROR: {str(e)}".encode())

    def handle_tcp_download(self, conn, client_id, cmd):
        try:
            parts = cmd.split()
            if len(parts) < 2:
                conn.sendall(b"Invalid DOWNLOAD command")
                return

            filename = parts[1]
            if not os.path.exists(filename):
                conn.sendall(b"File not found")
                return

            filesize = os.path.getsize(filename)
            conn.sendall(f"READY {filesize}".encode())

            with open(filename, "rb") as f:
                while True:
                    chunk = f.read(4096)
                    if not chunk:
                        break
                    conn.sendall(chunk)
            conn.sendall(b"DOWNLOAD COMPLETE")
        except Exception as e:
            conn.sendall(f"DOWNLOAD ERROR: {str(e)}".encode())

    def handle_udp_file_info(self, sock, client_id, payload, addr):
        try:
            null_pos = payload.find(b"\x00")
            if null_pos == -1:
                return

            filename = payload[:null_pos].decode()
            filesize = int(payload[null_pos + 1 :].decode())

            self.active_transfers[client_id] = {
                "filename": filename,
                "filesize": filesize,
                "received": 0,
                "temp_path": os.path.join(
                    self.partial_dir, f"udp_{client_id}_{filename}"
                ),
            }

            with open(self.active_transfers[client_id]["temp_path"], "wb") as f:
                pass  # Создаем пустой файл

            # Отправляем ACK
            header = struct.pack("!BBHH", 2, 0, 0, 1)  # ACK для первого пакета
            sock.sendto(header, addr)
        except Exception as e:
            print(f"UDP file info error: {e}")

    def handle_udp_data(self, sock, client_id, seq, data, addr):
        if client_id not in self.active_transfers:
            return

        transfer = self.active_transfers[client_id]
        try:
            with open(transfer["temp_path"], "ab") as f:
                f.write(data)
            transfer["received"] += len(data)

            # Отправляем ACK
            header = struct.pack("!BBHH", 2, 0, 0, seq)
            sock.sendto(header, addr)

            # Проверяем завершение
            if transfer["received"] >= transfer["filesize"]:
                self.complete_udp_transfer(client_id)
        except Exception as e:
            print(f"UDP data error: {e}")

    def complete_udp_transfer(self, client_id):
        if client_id not in self.active_transfers:
            return

        transfer = self.active_transfers[client_id]
        try:
            if transfer["received"] == transfer["filesize"]:
                os.replace(transfer["temp_path"], transfer["filename"])
                print(f"UDP transfer complete: {transfer['filename']}")
            else:
                print(
                    f"UDP transfer incomplete: {transfer['filename']} {transfer['received']}/{transfer['filesize']}"
                )
        finally:
            if client_id in self.active_transfers:
                del self.active_transfers[client_id]


if __name__ == "__main__":
    server = FileServer()
    server.start()
