import asyncio
import pandas as pd
import os
from panoramisk import Manager

from dotenv import load_dotenv
load_dotenv()
# --- CONFIGURACIÓN ---
# Solo cambia el nombre del archivo si subes uno nuevo
RUTA_EXCEL = '/root/dialer-scripts/excels/TestNums.xlsx' 
COLUMNA_TELEFONO = 'Telefono' # Nombre exacto de la columna

AMI_CONFIG = {
    'host': os.getenv('AMI_HOST', '127.0.0.1'),
    'port': int(os.getenv('AMI_PORT')), 
    'username': os.getenv('AMI_USER'),
    'secret': os.getenv('AMI_PASS')
}

EXTEN_AGENTE = '100' # Extensión de Liz

async def cargar_datos(ruta):
    if not os.path.exists(ruta):
        print(f"[X] ERROR: No existe el archivo en {ruta}")
        return None
        
    ext = os.path.splitext(ruta)[1].lower()
    try:
        if ext == '.csv':
            return pd.read_csv(ruta, sep=None, engine='python')
        elif ext == '.xlsx':
            # Parche específico para el error de tu imagen
            return pd.read_excel(ruta, engine='openpyxl')
        elif ext == '.xls':
            return pd.read_excel(ruta)
        else:
            print(f"[X] Formato {ext} no soportado.")
            return None
    except Exception as e:
        print(f"[X] Error al leer el archivo {ext}: {e}")
        return None

async def lanzar_llamada(manager, numero):
    """Limpia el número y envía la acción a Asterisk"""
    # Limpia formatos de Excel (quita .0 de los números)
    tel_limpio = str(numero).split('.')[0].strip()
    
    if not tel_limpio or tel_limpio == 'nan' or tel_limpio == '':
        return

    print(f"[*] Marcando a: {tel_limpio}...")
    
    action = {
        'Action': 'Originate',
        'Channel': f'Local/{tel_limpio}@from-internal',
        'Context': 'from-internal',
        'Exten': EXTEN_AGENTE,
        'Priority': '1',
        'Async': 'true'
    }

    try:
        response = await manager.send_action(action)
        print(f"[+] Orden enviada para {tel_limpio}: {response.Message}")
    except Exception as e:
        print(f"[!] Fallo al conectar llamada {tel_limpio}: {e}")

async def main():
    # 1. Cargar el archivo según su extensión
    df = await cargar_datos(RUTA_EXCEL)
    if df is None: return

    # 2. Validar que la columna exista
    if COLUMNA_TELEFONO not in df.columns:
        print(f"[X] ERROR: No existe la columna '{COLUMNA_TELEFONO}'")
        print(f"Columnas encontradas: {df.columns.tolist()}")
        return

    lista_numeros = df[COLUMNA_TELEFONO].dropna().tolist()
    print(f"[!] {len(lista_numeros)} números listos para marcar.")

    # 3. Conexión AMI y Bucle de marcación
    manager = Manager(**AMI_CONFIG)
    try:
        await manager.connect()
        print("[!] Conexión exitosa con Asterisk.")

        for tel in lista_numeros:
            await lanzar_llamada(manager, tel)
            print(f"[*] Esperando 45s para la siguiente llamada...")
            await asyncio.sleep(45) # Tiempo de gestión para Liz
            
    except Exception as e:
        print(f"[X] Error de conexión AMI: {e}")
    finally:
        await manager.disconnect()
        print("[!] Proceso finalizado.")

if __name__ == "__main__":
    # Compatible con Python 3.6 (Issabel estándar)
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())