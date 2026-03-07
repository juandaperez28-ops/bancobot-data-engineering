import subprocess
import time
import requests

def esperar_minio():
    print("Esperando a MinIO...")
    
    while True:
        try:
            r = requests.get("http://minio:9000/minio/health/live")
            if r.status_code == 200:
                print("MinIO listo")
                break
        except:
            pass
        
        time.sleep(2)

esperar_minio()

print("Inicializando bucket...")
subprocess.run(["python", "infrastructure/init_minio.py"], check=True)

print("Generando transacciones...")
subprocess.run(["python", "etl/etl_txn.py"], check=True)

print("Generando usuarios...")
subprocess.run(["python", "etl/etl_users.py"], check=True)

print("Iniciando bot...")
subprocess.run(["python", "-u", "-m", "bot.chatico_bot"], check=True)