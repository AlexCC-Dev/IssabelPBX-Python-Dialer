import os
import re
import json
import socket
import time
from datetime import datetime, timezone

import psycopg  # psycopg3

from dotenv import load_dotenv #Librería para lectura de archivos .env

load_dotenv() #Inicialización para lectura de .env

# =========================
# CONFIG (ajusta aquí)
# =========================
AMI_HOST = os.getenv("AMI_HOST")
AMI_PORT = int(os.getenv("AMI_PORT"))
AMI_USER = os.getenv("AMI_USER")
AMI_PASS = os.getenv("AMI_PASS")

PG_HOST = os.getenv("os.PG_HOST") 
PG_PORT = int(os.getenv("PG_PORT"))                    
PG_DB   = os.getenv("PG_DB")
PG_USER = os.getenv("PG_USER")
PG_PASS = os.getenv("PG_PASS")


def now_utc():
    return datetime.now(timezone.utc)


def normalize_mx_phone10(value: str | None) -> str | None:
    if not value:
        return None
    digits = re.sub(r"\D", "", str(value))

    if digits.startswith("01152"):
        digits = digits[5:]
    if digits.startswith("01") and len(digits) > 10:
        digits = digits[2:]
    if digits.startswith("52") and len(digits) >= 12:
        digits = digits[2:]

    if len(digits) >= 10:
        return digits[-10:]
    return None


def ami_login(sock: socket.socket):
    msg = (
        "Action: Login\r\n"
        f"Username: {AMI_USER}\r\n"
        f"Secret: {AMI_PASS}\r\n"
        "Events: on\r\n\r\n"
    )
    sock.sendall(msg.encode("utf-8"))


def ami_packets(sock: socket.socket):
    buf = b""
    while True:
        chunk = sock.recv(4096)
        if not chunk:
            raise ConnectionError("AMI connection closed")
        buf += chunk
        while b"\r\n\r\n" in buf:
            raw, buf = buf.split(b"\r\n\r\n", 1)
            yield raw.decode("utf-8", errors="replace")


def parse_ami(raw: str) -> dict:
    d = {}
    for line in raw.splitlines():
        if ":" not in line:
            continue
        k, v = line.split(":", 1)
        d[k.strip()] = v.strip()
    return d


def pick_dialed_number(msg: dict) -> str | None:

    for k in ("DestCallerIDNum", "DialString", "Exten"):
        v = msg.get(k)
        if v:
            return v
    return None


def pick_src_ext(msg: dict) -> str | None:
    for k in ("CallerIDNum", "CallerID"):
        v = msg.get(k)
        if v:
            return v
    return None


def main():
    print("== AMI -> Postgres (Dayler) ==")
    print(f"AMI: {AMI_HOST}:{AMI_PORT} user={AMI_USER}")
    print(f"PG : {PG_HOST}:{PG_PORT} db={PG_DB} user={PG_USER}")

    pg_conninfo = f"host={PG_HOST} port={PG_PORT} dbname={PG_DB} user={PG_USER} password={PG_PASS}"

    with psycopg.connect(pg_conninfo) as pg:
        pg.autocommit = True

        while True:
            sock = None
            try:
                sock = socket.create_connection((AMI_HOST, AMI_PORT), timeout=10)
                sock.settimeout(None)
                ami_login(sock)

                for raw in ami_packets(sock):
                    msg = parse_ami(raw)

                    if "Response" in msg:
                        continue

                    event = msg.get("Event")
                    if event not in ("DialEnd",):
                        continue

                    dialed_raw = pick_dialed_number(msg)
                    dialed_10 = normalize_mx_phone10(dialed_raw)
                    src_ext = pick_src_ext(msg)
                    disposition = msg.get("DialStatus")
                    channel = msg.get("Channel")
                    uniqueid = msg.get("Uniqueid")
                    linkedid = msg.get("Linkedid")

                    contacto_id = None
                    numero_contrato = None
                    contacto_nombre = None

                    if dialed_10:
                        with pg.cursor() as cur:
                            cur.execute(
                                """
                                SELECT id, numero_contrato, nombre, apellido_paterno
                                FROM contactos
                                WHERE celular_10 = %s
                                """,
                                (dialed_10,)
                            )
                            row = cur.fetchone()
                            if row:
                                contacto_id = row[0]
                                numero_contrato = row[1]
                                contacto_nombre = f"{row[2]} {row[3] or ''}".strip()

                    with pg.cursor() as cur:
                        cur.execute(
                            """
                            INSERT INTO call_events (
                                event_time, uniqueid, linkedid, src_extension,
                                dialed_raw, dialed_10, disposition, channel,
                                contacto_id, numero_contrato, contacto_nombre, extra
                            )
                            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                            """,
                            (
                                now_utc(),
                                uniqueid,
                                linkedid,
                                src_ext,
                                dialed_raw,
                                dialed_10,
                                disposition,
                                channel,
                                contacto_id,
                                numero_contrato,
                                contacto_nombre,
                                json.dumps(msg),
                            )
                        )

                    if dialed_10 and numero_contrato:
                        print(f"[MATCH] ext={src_ext} marcó={dialed_raw} -> {dialed_10} contrato={numero_contrato} disp={disposition}")
                    elif dialed_10:
                        print(f"[NO MATCH] ext={src_ext} marcó={dialed_raw} -> {dialed_10} disp={disposition}")

            except Exception as e:
                print("ERROR:", e)
                time.sleep(3)
            finally:
                try:
                    if sock:
                        sock.close()
                except Exception:
                    pass


if __name__ == "__main__":
    main()
