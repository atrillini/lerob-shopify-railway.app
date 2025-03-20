from minio import Minio
from minio.error import S3Error
import os

# Configura il client MinIO
minio_client = Minio(
    endpoint="bucket-production-51bd.up.railway.app:443",  # Solo il dominio, senza https://
    access_key=os.getenv("MINIO_ROOT_USER"),       # Credenziali da variabili d'ambiente
    secret_key=os.getenv("MINIO_ROOT_PASSWORD"),
    secure=True                                    # True per HTTPS (Railway usa 443)
)

# Nome del bucket da usare
bucket_name = "lerob-orders"

# Funzione per creare un bucket se non esiste
def create_bucket():
    try:
        if not minio_client.bucket_exists(bucket_name):
            minio_client.make_bucket(bucket_name)
            print(f"Bucket '{bucket_name}' creato con successo.")
        else:
            print(f"Bucket '{bucket_name}' esiste già.")
    except S3Error as err:
        print(f"Errore nella creazione del bucket: {err}")

# Funzione per caricare un file
def upload_file():
    try:
        local_file = "path/to/local/file.txt"  # Percorso del file locale
        object_name = "file.txt"               # Nome del file nel bucket
        
        minio_client.fput_object(
            bucket_name=bucket_name,
            object_name=object_name,
            file_path=local_file
        )
        print(f"File '{object_name}' caricato con successo in '{bucket_name}'.")
    except S3Error as err:
        print(f"Errore nel caricamento del file: {err}")

# Funzione per scaricare un file
def download_file():
    try:
        object_name = "file.txt"
        local_destination = "downloaded-file.txt"
        
        minio_client.fget_object(
            bucket_name=bucket_name,
            object_name=object_name,
            file_path=local_destination
        )
        print(f"File '{object_name}' scaricato con successo come '{local_destination}'.")
    except S3Error as err:
        print(f"Errore nello scaricamento del file: {err}")

# Funzione per creare e caricare un file
def create_and_upload_file():
    try:
        # Crea un file temporaneo
        local_file = "/tmp/test-file.txt"  # Usa /tmp per filesystem temporaneo su Railway
        content = "Questo è un file di test creato con Python!"
        
        # Scrivi il file localmente
        with open(local_file, "w") as f:
            f.write(content)
        
        # Carica il file su MinIO
        object_name = "test-file.txt"
        minio_client.fput_object(
            bucket_name=bucket_name,
            object_name=object_name,
            file_path=local_file
        )
        print(f"File '{object_name}' caricato con successo in '{bucket_name}'.")
        
        # Pulisci (opzionale)
        os.remove(local_file)
    except S3Error as err:
        print(f"Errore nel caricamento del file: {err}")
    except OSError as err:
        print(f"Errore nella creazione del file: {err}")

# Esegui
if __name__ == "__main__":
    create_bucket()
    create_and_upload_file()

# Esegui le funzioni
if __name__ == "__main__":
    create_bucket()
    create_and_upload_file()
