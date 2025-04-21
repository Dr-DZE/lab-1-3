import socket
import os
import time
import sys
import struct
from threading import Thread, Lock


class UDPFileTransfer:
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.settimeout(5)
        self.seq_num = 0
        self.window_size = 4
        self.lock = Lock()

    def send_packet(self, data, ptype=1, seq=None):
        with self.lock:
            if seq is None:
                self.seq_num = (self.seq_num + 1) % 65536
                seq = self.seq_num

            header = struct.pack("!BBHH", ptype, 0, seq, 0)
            self.sock.sendto(header + data, (self.host, self.port))
            return seq

    def receive_ack(self, timeout=1):
        try:
            data, _ = self.sock.recvfrom(1024)
            if len(data) >= 6:
                ptype, _, _, ack = struct.unpack("!BBHH", data[:6])
                if ptype == 2:  # ACK
                    return ack
        except socket.timeout:
            pass
        return None

    def upload_file(self, filename):
        if not os.path.exists(filename):
            print(f"File not found: {filename}")
            return False

        basename = os.path.basename(filename)
        filesize = os.path.getsize(filename)
        file_info = f"{basename}\0{filesize}".encode()

        # Отправляем информацию о файле
        seq = self.send_packet(file_info, ptype=4)
        if not self.receive_ack(5):
            print("No response from server")
            return False

        window = {}
        base_seq = seq + 1
        bytes_sent = 0

        with open(filename, "rb") as f:
            while bytes_sent < filesize:
                # Заполняем окно
                while len(window) < self.window_size and bytes_sent < filesize:
                    chunk = f.read(4096)
                    if chunk:
                        seq = self.send_packet(chunk)
                        window[seq] = (chunk, time.time())
                        bytes_sent += len(chunk)
                        print(
                            f"Progress: {bytes_sent}/{filesize} bytes ({bytes_sent/filesize:.1%})",
                            end="\r",
                        )

                # Проверяем ACK
                ack = self.receive_ack()
                if ack is not None:
                    window = {k: v for k, v in window.items() if k > ack}

                # Повторная отправка устаревших пакетов
                current_time = time.time()
                for seq, (chunk, send_time) in list(window.items()):
                    if current_time - send_time > 1.0:
                        self.send_packet(chunk, seq_num=seq)
                        window[seq] = (chunk, current_time)

        # Завершаем передачу
        retries = 0
        while retries < 3:
            self.send_packet(b"", ptype=5)
            ack = self.receive_ack()
            if ack is not None:
                print("\nUpload completed successfully")
                return True
            retries += 1

        print("\nUpload finished but no final ACK received")
        return False


class FileTransferClient:
    def __init__(self, host="localhost", tcp_port=12345, udp_port=12346):
        self.host = host
        self.tcp_port = tcp_port
        self.udp_port = udp_port
        self.tcp_socket = None
        self.udp_client = UDPFileTransfer(host, udp_port)
        self.partial_dir = "partial_downloads"
        os.makedirs(self.partial_dir, exist_ok=True)

    def connect(self):
        try:
            self.tcp_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.tcp_socket.connect((self.host, self.tcp_port))
            return True
        except Exception as e:
            print(f"Connection error: {e}")
            return False

    def transfer_file(self, filename, mode, use_udp=False):
        if mode == "UPLOAD" and not os.path.exists(filename):
            print(f"File not found: {filename}")
            return False

        if use_udp:
            if mode == "UPLOAD":
                return self.udp_client.upload_file(filename)
            else:
                print("UDP download not implemented")
                return False

        # TCP передача
        try:
            cmd = f"{mode} {os.path.basename(filename)}"
            if mode == "UPLOAD":
                cmd += f" {os.path.getsize(filename)}"

            self.tcp_socket.sendall(cmd.encode())
            response = self.tcp_socket.recv(1024).decode()

            if not response.startswith(("READY", "RESUME")):
                print(f"Server error: {response}")
                return False

            if mode == "UPLOAD":
                with open(filename, "rb") as f:
                    if response.startswith("RESUME"):
                        resume_pos = int(response.split()[1])
                        f.seek(resume_pos)

                    while True:
                        chunk = f.read(4096)
                        if not chunk:
                            break
                        self.tcp_socket.sendall(chunk)

                final_response = self.tcp_socket.recv(1024).decode()
                print(final_response)
                return "COMPLETE" in final_response

            else:  # DOWNLOAD
                if response.startswith("READY"):
                    filesize = int(response.split()[1])
                else:
                    print(f"Invalid server response: {response}")
                    return False

                temp_path = os.path.join(self.partial_dir, f"temp_{filename}")
                received = 0

                with open(temp_path, "wb") as f:
                    while received < filesize:
                        data = self.tcp_socket.recv(min(4096, filesize - received))
                        if not data:
                            break
                        f.write(data)
                        received += len(data)
                        print(
                            f"Progress: {received}/{filesize} bytes ({received/filesize:.1%})",
                            end="\r",
                        )

                final_response = self.tcp_socket.recv(1024).decode()
                if "COMPLETE" in final_response and received == filesize:
                    os.replace(temp_path, filename)
                    print("\nDownload completed successfully")
                    return True
                else:
                    print(f"\nDownload failed: {final_response}")
                    return False

        except Exception as e:
            print(f"Transfer error: {e}")
            return False

    def interactive(self):
        print(f"Connected to {self.host} (TCP:{self.tcp_port}, UDP:{self.udp_port})")
        print("Commands: UPLOAD <file> [udp], DOWNLOAD <file> [udp], EXIT")

        while True:
            try:
                cmd = input("> ").strip()
                if not cmd:
                    continue

                if cmd.upper() in ("EXIT", "QUIT"):
                    break

                elif cmd.upper().startswith(("UPLOAD", "DOWNLOAD")):
                    parts = cmd.split()
                    if len(parts) < 2:
                        print(f"Usage: {parts[0]} filename [udp]")
                        continue

                    use_udp = len(parts) > 2 and parts[2].lower() == "udp"
                    if self.transfer_file(parts[1], parts[0].upper(), use_udp):
                        print("Success")
                    else:
                        print("Failed")

                else:
                    print("Unknown command")

            except KeyboardInterrupt:
                print("\nClosing connection...")
                break
            except Exception as e:
                print(f"Error: {e}")
                break


if __name__ == "__main__":
    host = sys.argv[1] if len(sys.argv) > 1 else "localhost"
    tcp_port = int(sys.argv[2]) if len(sys.argv) > 2 else 12345
    udp_port = tcp_port + 1

    client = FileTransferClient(host, tcp_port, udp_port)
    if client.connect():
        client.interactive()
    else:
        print(f"Failed to connect to {host}:{tcp_port}")
