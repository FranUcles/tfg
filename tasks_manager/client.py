#!/usr/bin/env python3
import socket
import sys
import struct
import json

def client_tcp(host, port, mensaje):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    try:
        s.connect((host, port))
        s.sendall(mensaje)
        respuesta = s.recv(1024)
        print("Respuesta del servidor:", respuesta.decode())
    finally:
        s.close()


def client_unix(path, mensaje):
    s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)

    try:
        s.connect(path)
        s.sendall(mensaje)
        respuesta = s.recv(1024)
        print("Respuesta del servidor:", respuesta.decode())
    finally:
        s.close()

def create_packet(file_payload):
    # Leer mensaje desde fichero JSON
    try:
        with open(file_payload, 'r') as f:
            data = json.load(f)
            message = json.dumps(data)  # convierte objeto JSON a string
    except Exception as e:
        print("Error leyendo JSON:", e)
        sys.exit(1)

    payload = json.dumps(data).encode("utf-8")  # Convertir a bytes UTF-8
    length = struct.pack("!I", len(payload))    # 4 bytes, big-endian
    return length + payload

def main():
    if len(sys.argv) < 3:
        print("Uso:")
        print("  python cliente.py tcp <host> <port> <file>")
        print("  python cliente.py unix <socket_path> <file>")
        sys.exit(1)

    modo = sys.argv[1]

    if modo == "tcp":
        if len(sys.argv) != 5:
            print("Uso: python cliente.py tcp <host> <port> <file>")
            sys.exit(1)
        host = sys.argv[2]
        port = int(sys.argv[3])
        packet = create_packet(sys.argv[4])
        client_tcp(host, port, packet)

    elif modo == "unix":
        if len(sys.argv) != 4:
            print("Uso: python cliente.py unix <socket_path> <file>")
            sys.exit(1)
        path = sys.argv[2]
        packet = create_packet(sys.argv[3])
        client_unix(path, packet)

    else:
        print("Modo inválido. Usa: tcp | unix")
        sys.exit(1)


if __name__ == "__main__":
    main()

