import requests
from bs4 import BeautifulSoup
import boto3
import uuid

def lambda_handler(event, context):

    url = "https://ultimosismo.igp.gob.pe/ultimo-sismo/sismos-reportados"

    # Pedir HTML
    response = requests.get(url)
    if response.status_code != 200:
        return {
            "statusCode": 500,
            "body": "No se pudo acceder a la página del IGP"
        }

    soup = BeautifulSoup(response.content, "html.parser")

    # Encontrar tabla. La web del IGP usa el ID "tabla_sismos"
    table = soup.find("table", {"id": "tabla_sismos"})
    if not table:
        return {
            "statusCode": 404,
            "body": "No se encontró la tabla de sismos"
        }

    # Extraer encabezados
    headers = [th.text.strip() for th in table.find_all("th")]

    # Extraer filas (solo 10)
    rows = []
    for row in table.find("tbody").find_all("tr")[:10]:
        cols = [td.text.strip() for td in row.find_all("td")]
        rows.append({headers[i]: cols[i] for i in range(len(cols))})

    # Insertar en DynamoDB
    dynamodb = boto3.resource("dynamodb")
    table_db = dynamodb.Table("TablaSismos")

    # Borrar datos previos
    scan = table_db.scan()
    with table_db.batch_writer() as batch:
        for item in scan.get("Items", []):
            batch.delete_item(Key={"id": item["id"]})

    # Insertar nuevos sismos
    for r in rows:
        r["id"] = str(uuid.uuid4())
        table_db.put_item(Item=r)

    return {
        "statusCode": 200,
        "body": rows
    }
