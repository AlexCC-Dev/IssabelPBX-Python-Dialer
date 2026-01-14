import asyncio
import psycopg2
import os
from panoramisk import Manager
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

load_dotenv()

# --- CONFIGURACIÓN AMI (Asterisk) ---
AMI_CONFIG = {
    'host': os.getenv('AMI_HOST', '127.0.0.1'),
    'port': int(os.getenv('AMI_PORT')), 
    'username': os.getenv('AMI_USER'),
    'secret': os.getenv('AMI_PASS')
}

# --- CONFIGURACIÓN POSTGRES ---
DB_CONFIG = {
    "host": os.getenv("BD_HOST"),
    "database": os.getenv("PG_DB"),
    "user": os.getenv("PG_USER"),
    "password": os.getenv("PG_PASS"),
    "port": int(os.getenv("PG_PORT"))
}

EXTEN_AGENTE = '100' # Extensión de Liz

def obtener_socios_de_bd():
    """Conecta a la BD y trae a los socios con teléfonos válidos."""
    query = """
        SELECT nombre, contrato, telefono_1, telefono_2 
        FROM socios 
        WHERE (telefono_1 IS NOT NULL AND telefono_1 <> '')
           OR (telefono_2 IS NOT NULL AND telefono_2 <> '');
    """
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        # RealDictCursor nos permite acceder a los datos por nombre de columna: socio['nombre']
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute(query)
        socios = cur.fetchall()
        cur.close()
        conn.close()
        return socios
    except Exception as e:
        print(f"[X] ERROR al conectar con Postgres: {e}")
        return []

async def lanzar_llamada(manager, numero, nombre, contrato):
    """Limpia el número y envía la acción Originate a Asterisk"""
    tel_limpio = str(numero).strip()
    
    if not tel_limpio or tel_limpio.lower() == 'nan' or len(tel_limpio) < 7:
        return False

    print(f"[*] Marcando a: {nombre} ({tel_limpio}) | Contrato: {contrato}")
    
    action = {
        'Action': 'Originate',
        'Channel': f'Local/{tel_limpio}@from-internal',
        'Context': 'from-internal',
        'Exten': EXTEN_AGENTE,
        'Priority': '1',
        'Async': 'true',
        'Variable': f'SOCIO_NOMBRE={nombre},SOCIO_CONTRATO={contrato}' # Variables útiles para el log de Asterisk
    }

    try:
        response = await manager.send_action(action)
        print(f"[+] Orden enviada para {nombre}: {response.Message}")
        return True
    except Exception as e:
        print(f"[!] Fallo al conectar llamada con {nombre}: {e}")
        return False

async def main():
    # 1. Obtener datos de la Base de Datos
    lista_socios = obtener_socios_de_bd()
    
    if not lista_socios:
        print("[X] No hay socios con números de teléfono válidos para marcar.")
        return

    print(f"[!] {len(lista_socios)} socios recuperados de la BD.")

    # 2. Conexión AMI
    manager = Manager(**AMI_CONFIG)
    try:
        await manager.connect()
        print("[!] Conexión exitosa con Asterisk (AMI).")

        for socio in lista_socios:
            # Procesar Teléfono 1
            if socio['telefono_1']:
                await lanzar_llamada(manager, socio['telefono_1'], socio['nombre'], socio['contrato'])
                print(f"[*] Esperando 45s para la siguiente gestión...")
                await asyncio.sleep(45)

            # Procesar Teléfono 2 (si existe)
            if socio['telefono_2']:
                print(f"[*] Socio {socio['nombre']} tiene un segundo número...")
                await lanzar_llamada(manager, socio['telefono_2'], socio['nombre'], socio['contrato'])
                print(f"[*] Esperando 45s para la siguiente gestión...")
                await asyncio.sleep(45)
            
    except Exception as e:
        print(f"[X] Error de conexión AMI: {e}")
    finally:
        await manager.disconnect()
        print("[!] Proceso de marcación finalizado.")

if __name__ == "__main__":
    # Ejecución del bucle de eventos
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())