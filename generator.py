import pandas as pd
import csv
import chardet
import re
import unicodedata


def detectar_encoding(ruta_archivo):

    with open(ruta_archivo, "rb") as f:
        resultado = chardet.detect(f.read(10000))

    return resultado["encoding"]


def detectar_delimitador(ruta_archivo):

    with open(ruta_archivo, "r", encoding="utf-8") as f:
        muestra = f.read(2048)

        sniffer = csv.Sniffer()
        dialecto = sniffer.sniff(muestra)

        return dialecto.delimiter


def limpiar_nombre_columna(nombre):

    nombre = nombre.strip()

    nombre = quitar_acentos(nombre)

    nombre = nombre.lower()

    nombre = nombre.replace(" ", "_")

    nombre = re.sub(r"[^\w_]", "", nombre)

    return nombre


def limpiar_nombre_tabla(nombre):

    nombre = nombre.strip()

    nombre = quitar_acentos(nombre)

    nombre = nombre.lower()

    nombre = nombre.replace(" ", "_")

    nombre = re.sub(r"[^\w_]", "", nombre)

    return nombre


def leer_archivo(ruta_archivo):

    encoding = detectar_encoding(ruta_archivo)

    if ruta_archivo.endswith(".csv") or ruta_archivo.endswith(".txt"):

        delimitador = detectar_delimitador(ruta_archivo)

        df = pd.read_csv(
            ruta_archivo,
            delimiter=delimitador,
            encoding=encoding,
            engine="python"
        )

    elif ruta_archivo.endswith(".xlsx"):

        df = pd.read_excel(ruta_archivo)

    else:

        raise ValueError("Formato de archivo no soportado")

    # limpiar columnas
    df.columns = [limpiar_nombre_columna(col) for col in df.columns]

    return df


def detectar_tipo_sql(serie):

    if pd.api.types.is_integer_dtype(serie):
        return "INT"

    elif pd.api.types.is_float_dtype(serie):
        return "DECIMAL(18,4)"

    elif pd.api.types.is_bool_dtype(serie):
        return "BOOLEAN"

    elif pd.api.types.is_datetime64_any_dtype(serie):
        return "DATETIME"

    else:

        max_len = serie.astype(str).map(len).max()

        if max_len < 50:
            return "VARCHAR(50)"

        elif max_len < 255:
            return "VARCHAR(255)"

        else:
            return "TEXT"


def generar_create_table(df, tabla):

    sql = f"CREATE TABLE {tabla} (\n"

    for col in df.columns:

        tipo_sql = detectar_tipo_sql(df[col])

        sql += f"  {col} {tipo_sql},\n"

    sql = sql.rstrip(",\n")

    sql += "\n);"

    return sql


def generar_insert(df, tabla):

    columnas = ", ".join(df.columns)

    sql = f"INSERT INTO {tabla} ({columnas}) VALUES\n"

    valores_sql = []

    for _, row in df.iterrows():

        valores = []

        for v in row:

            if pd.isna(v):
                valores.append("NULL")

            elif isinstance(v, str):
                v = v.replace("'", "''")
                valores.append(f"'{v}'")

            else:
                valores.append(str(v))

        valores_sql.append("(" + ", ".join(valores) + ")")

    sql += ",\n".join(valores_sql)

    sql += ";"

    return sql

def quitar_acentos(texto):

    texto = unicodedata.normalize('NFKD', texto)
    texto = texto.encode('ascii', 'ignore').decode('ascii')

    return texto