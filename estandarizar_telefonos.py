import pandas as pd
import re
import psycopg2
import os
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

# ==========================================
# 1. CONFIGURACIÓN
# ==========================================
DB_CONFIG = {
    "host": os.getenv("BD_HOST"),
    "database": os.getenv("PG_DB"),
    "user": os.getenv("PG_USER"),
    "password": os.getenv("PG_PASS"),
    "port": int(os.getenv("PG_PORT"))
}

RUTA_ENTRADA = "./entradas/"
RUTA_SALIDA = "./salidas/"

# ==========================================
# 2. FUNCIONES DE APOYO (Validadores)
# ==========================================

def limpiar_telefono(valor):
    if pd.isna(valor) or str(valor).strip() == "" or str(valor).lower() in ['nan', 'na']:
        return None
    solo_numeros = re.sub(r'\D', '', str(valor))
    return solo_numeros[-10:] if len(solo_numeros) >= 10 else solo_numeros

def safe_int(valor):
    """Convierte a entero de forma segura. Si es 'NA' o texto, devuelve None."""
    try:
        if pd.isna(valor) or str(valor).strip().lower() in ['nan', 'na', 'n/a']:
            return None
        # Quitamos cualquier carácter no numérico por si acaso
        num_str = re.sub(r'\D', '', str(valor))
        return int(num_str) if num_str else None
    except:
        return None

def safe_decimal(valor):
    """Convierte a flotante/decimal de forma segura."""
    try:
        if pd.isna(valor) or str(valor).strip().lower() in ['nan', 'na', 'n/a']:
            return 0.0
        return float(valor)
    except:
        return 0.0

# ==========================================
# 3. ALGORITMOS DE BD
# ==========================================

def algoritmo_escritura_registro(cur, df):
    catalogos = {
        'Pais Socio': ('paises', 'nombre_pais'),
        'Estado Socio': ('estados', 'nombre_estado'),
        'Ciudad Socio': ('ciudades', 'nombre_ciudad'),
        'Idioma': ('idiomas', 'nombre_idioma'),
        'Moneda': ('monedas', 'codigo_moneda'),
        'Tipo Tarjeta': ('tipos_tarjeta', 'nombre_tipo'),
        'Tipo cobro': ('tipos_cobro', 'nombre_cobro')
    }
    for col_excel, (tabla_bd, col_bd) in catalogos.items():
        if col_excel in df.columns:
            valores_unicos = df[col_excel].dropna().unique()
            for valor in valores_unicos:
                val_str = str(valor).strip()
                if val_str == "" or val_str.lower() in ['nan', 'na']: continue
                if col_excel == 'Moneda': val_str = val_str[:3].upper()

                cur.execute(f"SELECT 1 FROM {tabla_bd} WHERE {col_bd} = %s", (val_str,))
                if not cur.fetchone():
                    cur.execute(f"INSERT INTO {tabla_bd} ({col_bd}) VALUES (%s)", (val_str,))

def algoritmo_comparar_asignar(cur, tabla, col_nombre_bd, val_excel):
    if pd.isna(val_excel) or str(val_excel).strip() == "" or str(val_excel).lower() in ['nan', 'na']:
        return None
    val_busqueda = str(val_excel).strip()
    if tabla == 'monedas': val_busqueda = val_busqueda[:3].upper()

    mapeo_ids = {
        'paises': 'id_pais', 'estados': 'id_estado', 'ciudades': 'id_ciudad',
        'idiomas': 'id_idioma', 'monedas': 'id_moneda',
        'tipos_tarjeta': 'id_tipo_tarjeta', 'tipos_cobro': 'id_tipo_cobro'
    }
    cur.execute(f"SELECT {mapeo_ids[tabla]} FROM {tabla} WHERE {col_nombre_bd} = %s", (val_busqueda,))
    res = cur.fetchone()
    return res[0] if res else None

# ==========================================
# 4. PROCESAMIENTO PRINCIPAL
# ==========================================

def procesar_archivo_hibrido(nombre_archivo):
    ruta_completa = os.path.join(RUTA_ENTRADA, nombre_archivo)
    df = pd.read_excel(ruta_completa)
    df.columns = df.columns.str.replace(r'\s+', ' ', regex=True).str.strip()

    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()

        algoritmo_escritura_registro(cur, df)
        conn.commit()

        for index, row in df.iterrows():
            try:
                # 1. Estandarización de Apellidos
                ape_p, ape_m = str(row.get('Apellido Paterno','')).strip(), str(row.get('Apellido Materno','')).strip()
                apellido = f"{ape_p} {ape_m}".strip() if ape_m.lower() not in ['nan', 'na', ''] else ape_p
                
                # 2. Split de Teléfonos y Emails
                t1, t2 = (limpiar_telefono(x) for x in (str(row.get('Celular','')).split(',') + [None])[:2])
                m1, m2 = (str(x).strip() if x else None for x in (str(row.get('EMail','')).split(',') + [None])[:2])

                # 3. Evitar duplicados de Correo (Regla 36)
                if m1:
                    cur.execute("SELECT 1 FROM socios WHERE correo_1 = %s", (m1,))
                    if cur.fetchone():
                        print(f"Fila {index}: Correo {m1} ya existe. Omitiendo.")
                        continue

                # 4. Obtención de FKs
                fks = {
                    'p': algoritmo_comparar_asignar(cur, 'paises', 'nombre_pais', row.get('Pais Socio')),
                    'e': algoritmo_comparar_asignar(cur, 'estados', 'nombre_estado', row.get('Estado Socio')),
                    'c': algoritmo_comparar_asignar(cur, 'ciudades', 'nombre_ciudad', row.get('Ciudad Socio')),
                    'i': algoritmo_comparar_asignar(cur, 'idiomas', 'nombre_idioma', row.get('Idioma')),
                    'm': algoritmo_comparar_asignar(cur, 'monedas', 'codigo_moneda', row.get('Moneda')),
                    'tt': algoritmo_comparar_asignar(cur, 'tipos_tarjeta', 'nombre_tipo', row.get('Tipo Tarjeta')),
                    'tc': algoritmo_comparar_asignar(cur, 'tipos_cobro', 'nombre_cobro', row.get('Tipo cobro'))
                }

                # 5. Inserción con Safe Casting (Resuelve NA y Out of Range)
                cur.execute("""
                    INSERT INTO socios (
                        nombre, apellido, fecha_venta, contrato, codigo_postal, direccion_socio,
                        telefono_1, telefono_2, correo_1, correo_2, titular, numero_tarjeta,
                        mes_tarjeta, anio_tarjeta, volumen_venta, pagare_total, plazo_total,
                        tasa_interes, importe_mensualidad_intereses, fecha_compromiso_pago,
                        mensualidades_pendientes_cobro, fecha_ultima_mensualidad_pagada,
                        fecha_siguiente_mensualidad, mensualidades_vencidas,
                        id_pais, id_estado, id_ciudad, id_idioma, id_moneda, id_tipo_tarjeta, id_tipo_cobro
                    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """, (
                    row.get('Nombre Socio'), apellido, row.get('Fecha Venta'), str(row.get('Contrato'))[:15], 
                    safe_int(row.get('Codigo Postal')), row.get('Direccion Socio'),
                    t1, t2, m1, m2, row.get('Titular'), row.get('Numero Tarjeta'),
                    safe_int(row.get('Mes Tarjeta')), safe_int(row.get('Año Tarjeta')), 
                    safe_decimal(row.get('Volumen Venta')), safe_decimal(row.get('Pagare Total')), 
                    safe_int(row.get('Plazo Total')), safe_int(row.get('Tasa Interes')), 
                    safe_decimal(row.get('Importe Mensualidad Incluyendo Intereses')), row.get('Fecha Compromiso de Pago'),
                    safe_int(row.get('No. Mensualidades Pendientes de Cobro')), row.get('Fecha Ult. Mensualidad Pagada'),
                    row.get('Fecha Sig. Mensualidad'), safe_int(row.get('Men. Vencidas')),
                    fks['p'], fks['e'], fks['c'], fks['i'], fks['m'], fks['tt'], fks['tc']
                ))
            except Exception as e_row:
                conn.rollback()
                print(f"Error en fila {index}: {e_row}")

        conn.commit()
        cur.execute("SELECT count(*) FROM socios;")
        print(f"Carga finalizada. Registros en BD: {cur.fetchone()[0]}")

    except Exception as e:
        print(f"Fallo crítico: {e}")
    finally:
        if conn: conn.close()

if __name__ == "__main__":
    procesar_archivo_hibrido("Informacion_Usuarios.xls")