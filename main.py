import urllib.parse
from pymongo import MongoClient
import csv
import pymongo

username = urllib.parse.quote_plus("Lab03_DB")
password = urllib.parse.quote_plus("Lab03@2023")

client = MongoClient(
    f"mongodb+srv://{username}:{password}@laboratorio03.swr0jfq.mongodb.net/test",
    socketTimeoutMS=30000,
)

try:
    # Checquear la conexion a la base de datos
    print(client.list_database_names())
except Exception as e:
    print("Error connecting to the database:", e)

db = client["Laboratorio03"]
collection = db["winemag"]

# Leer archivo CSV
with open("winemag-data.csv", "r", encoding="utf-8") as file:
    reader = csv.DictReader(file)
    data = [row for row in reader]

# Dividir la data en chuncks de 2000 para poder enviar la data
chunk_size = 2000
for i in range(0, len(data), chunk_size):
    chunk = data[i : i + chunk_size]
    bulk_write = [pymongo.InsertOne(item) for item in chunk]
    result = collection.bulk_write(bulk_write)
    print("Inserted %d documents" % result.inserted_count)
