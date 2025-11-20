import requests
import boto3
import uuid

def lambda_handler(event, context):
    # API real del IGP
    url = "https://sismoapi.igp.gob.pe/api/sismo?page=1&limit=10"

    response = requests.get(url)
    if response.status_code != 200:
        return {
            "statusCode": response.status_code,
            "body": "Error al acceder a la API de sismos"
        }

    data = response.json()

    # Lista real de sismos
    sismos = data.get("data", [])

    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('TablaSismos')

    # Limpiar tabla antes de insertar nuevos registros
    scan = table.scan()
    with table.batch_writer() as batch:
        for item in scan['Items']:
            batch.delete_item(Key={'id': item['id']})

    # Guardamos
    for s in sismos:
        s["id"] = str(uuid.uuid4())
        table.put_item(Item=s)

    return {
        "statusCode": 200,
        "body": sismos
    }
