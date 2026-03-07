import dask.dataframe as dd
import pandas as pd
from datetime import datetime, timedelta
import random
import s3fs
import pytz

def GENERAR_DATOS_SIM():
    # ==============================
    # CONFIGURACION MINIO / S3
    # ==============================

    storage_options = {
        "key": "admin",
        "secret": "admin123",
        "client_kwargs": {
            "endpoint_url": "http://minio:9000"
        },
        "config_kwargs": {
            "s3": {"addressing_style": "path"}
        }
    }

    fs = s3fs.S3FileSystem(**storage_options)

    # ==============================
    # INTENTAR LEER HISTORICO
    # ==============================

    try:
        df_dask = dd.read_parquet(
            "s3://realbucket/movimientos_parquet/",
            storage_options=storage_options,
            engine="pyarrow"
        )

        saldos = (
            df_dask
            .groupby("usuario_id")["saldo"]
            .last()
            .compute()
            .to_dict()
        )

        print("HISTORICO CARGADO CORRECTAMENTE")

    except Exception:
        print("NO EXISTE HISTORICO, SE INICIA DESDE CERO")
        saldos = {}

    # ==============================
    # DETECTAR FECHAS EXISTENTES
    # ==============================

    archivos = fs.glob("realbucket/movimientos_parquet/TXN_*")

    fechas_existentes = []

    for path in archivos:

        # obtener solo el nombre de la carpeta
        nombre = path.split("/")[-1]

        if nombre.startswith("TXN_"):
            fecha_str = nombre.replace("TXN_", "")

            try:
                fecha = datetime.strptime(fecha_str, "%Y-%m-%d").date()
                fechas_existentes.append(fecha)
            except ValueError:
                pass
    
    if fechas_existentes:
        ultima_fecha = max(fechas_existentes)
        fecha_inicio = ultima_fecha + timedelta(days=1)
        print("Continuando desde:", fecha_inicio)
    else:
        colombia = pytz.timezone("America/Bogota")
        fecha_inicio = datetime.now(colombia).date()
        print("No hay histórico. Se inicia desde hoy:", fecha_inicio)

    # ==============================
    # LISTAS DESCRIPCIONES
    # ==============================

    DES_ENTRADA = [
        "ABONO NOMINA EMPRESA X S.A.S.",
        "TRANSFERENCIA RECIBIDA TERCERO",
        "CONSIGNACION EFECTIVO CORRESPONSAL",
        "REEMBOLSO GASTOS EMPRESA X"
    ]

    DES_SALIDA = [
        "TRANSFERENCIA A OTRO BANCO",
        "RETIRO CAJERO AUTOMATICO ATH",
        "COMPRA DEBITO COMERCIO",
        "PAGO TARJETA CREDITO PSE"
    ]

    # ==============================
    # PARAMETROS SIMULACION
    # ==============================

    TRANSACCIONES_POR_DIA = 90000
    DIAS_SIMULAR = 90

    colombia = pytz.timezone("America/Bogota")
    hoy = datetime.now(colombia).date()

    fecha_objetivo = hoy + timedelta(days=DIAS_SIMULAR)

    if fechas_existentes:
        ultima_fecha = max(fechas_existentes)
    else:
        ultima_fecha = hoy - timedelta(days=1)

    dias_faltantes = max(0, (fecha_objetivo - ultima_fecha).days)

    print("Última fecha generada:", ultima_fecha)
    print("Fecha objetivo:", fecha_objetivo)
    print("Días faltantes por generar:", dias_faltantes)

    # ==============================
    # LOOP POR CADA DIA
    # ==============================

    for dia in range(dias_faltantes):

        fecha_actual = fecha_inicio + timedelta(days=dia)

        datos = []

        for i in range(TRANSACCIONES_POR_DIA):

            usuario_id = random.randint(1, 45000)
            saldo = saldos.get(usuario_id, 0)

            tipo = random.choice(["ENTRADA", "SALIDA"])
            monto = random.randint(1000, 5000)*5

            if tipo == "ENTRADA":
                descripcion = random.choice(DES_ENTRADA)
                saldo += monto

            else:
                descripcion = random.choice(DES_SALIDA)

                if saldo == 0:
                    continue

                if monto > saldo:
                    monto = saldo

                saldo -= monto

            saldos[usuario_id] = saldo

            datos.append([
                usuario_id,
                fecha_actual,
                descripcion,
                tipo,
                monto,
                saldo
            ])

        df_dia = pd.DataFrame(datos, columns=[
            "usuario_id", "txn_date", "descripcion", "tipo", "monto", "saldo"
        ])

        df_dia["txn_date"] = df_dia["txn_date"].astype("string")

        ddf_dia = dd.from_pandas(df_dia, npartitions=10)

        ddf_dia.to_parquet(
            f"s3://realbucket/movimientos_parquet/TXN_{fecha_actual}",
            storage_options=storage_options,
            write_index=False
        )

        print(f"DIA {fecha_actual} GUARDADO - REGISTROS: {len(df_dia)}")

if __name__ == "__main__":
    GENERAR_DATOS_SIM()