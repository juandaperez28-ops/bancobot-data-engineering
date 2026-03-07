import boto3
import botocore


def GENERAR_CONEXION():

    s3 = boto3.client(
        "s3",
        endpoint_url="http://minio:9000",
        aws_access_key_id="admin",
        aws_secret_access_key="admin123"
    )

    bucket = "realbucket"

    response = s3.list_buckets()
    print("Buckets detectados:", response)
    buckets = [b["Name"] for b in response["Buckets"]]

    if bucket in buckets:
        print("BUCKET YA EXISTE")
    else:
        print("BUCKET NO EXISTE, CREANDO...")
        s3.create_bucket(Bucket=bucket)
        print("BUCKET CREADO CORRECTAMENTE")

if __name__ == "__main__":
    GENERAR_CONEXION()