import dask.dataframe as dd
import pandas as pd
import hashlib
import random
import time
import s3fs
import io

def hash_password(password):
    return hashlib.sha256(password.encode()).hexdigest()


def esperar_movimientos(storage_options):

    fs = s3fs.S3FileSystem(**storage_options)

    print("Esperando datos de movimientos...")

    while True:
        try:
            archivos = fs.glob("realbucket/movimientos_parquet/*")

            if len(archivos) > 0:
                print("Datos encontrados, continuando...")
                return

        except Exception:
            pass

        print("Aun no hay datos, esperando 5 segundos...")
        time.sleep(5)


def CREAR_USUARIOS():

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

    esperar_movimientos(storage_options)

    print("Leyendo movimientos...")

    df_dask = dd.read_parquet(
        "s3://realbucket/movimientos_parquet/",
        storage_options=storage_options,
        engine="pyarrow"
    )

    usuarios_ids = df_dask["usuario_id"].drop_duplicates().compute().tolist()

    print("Usuarios encontrados:", len(usuarios_ids))

    registros = []

    for uid in usuarios_ids:

        password = f"pass{uid}"
        pin = str(random.randint(1000, 9999))

        registros.append({
            "usuario_id": uid,
            "username": f"user{uid}@banco.com",
            "password_hash": hash_password(password),
            "pin": pin,
            "main_account": f"101010-{uid}"
        })

    df_users = pd.DataFrame(registros)

    ddf = dd.from_pandas(df_users, npartitions=1)

    ddf.to_parquet(
        "s3://realbucket/USERS/",
        storage_options=storage_options,
        write_index=False
    )

    print("Base de usuarios creada correctamente")

    # ================= GUARDAR CSV EN MINIO =================

    fs = s3fs.S3FileSystem(**storage_options)

    csv_buffer = io.StringIO()
    df_users.to_csv(csv_buffer, index=False)

    with fs.open("realbucket/USERS/users_disponibles.csv", "w") as f:
        f.write(csv_buffer.getvalue())

    print("CSV guardado en MinIO")

if __name__ == "__main__":
    CREAR_USUARIOS()