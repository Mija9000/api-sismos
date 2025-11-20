import json
import requests
from bs4 import BeautifulSoup
import boto3
import uuid

def parse_table(soup):
    # intenta encontrar una tabla con filas
    table = soup.find("table")
    if not table:
        return None
    headers = [th.get_text(strip=True) for th in table.find_all("th")]
    rows = []
    tbody = table.find("tbody") or table
    for tr in tbody.find_all("tr")[:10]:
        cols = [td.get_text(strip=True) for td in tr.find_all("td")]
        # si headers no coinciden, usa índices
        if headers and len(headers) == len(cols):
            rows.append({headers[i]: cols[i] for i in range(len(cols))})
        else:
            rows.append({str(i): cols[i] for i in range(len(cols))})
    return rows

def parse_cards(soup):
    # alternativa: páginas que usan "cards" o Views (Drupal/Wordpress)
    items = soup.select(".views-row, .sismo-item, .item")  # varios selectores probables
    results = []
    for item in items[:10]:
        text = item.get_text(" ", strip=True)
        results.append({"text": text})
    return results if results else None

def lambda_handler(event, context):
    url = "https://ultimosismo.igp.gob.pe/ultimo-sismo/sismos-reportados"
    try:
        resp = requests.get(url, timeout=12)
        resp.raise_for_status()
    except Exception as e:
        return {"statusCode": 502, "body": f"Error fetching page: {e}"}

    soup = BeautifulSoup(resp.text, "html.parser")

    # 1) intentar tabla
    rows = parse_table(soup)
    # 2) fallback a tarjetas / vistas
    if not rows:
        rows = parse_cards(soup)
    if not rows:
        return {"statusCode": 404, "body": "No se encontraron elementos a scrapear"}

    # preparar DynamoDB
    dynamodb = boto3.resource("dynamodb")
    table_name = "TablaSismos"  # cambia si quieres otro nombre
    table = dynamodb.Table(table_name)

    # borrar items previos (scan + batch delete)
    try:
        scan = table.scan()
        with table.batch_writer() as batch:
            for it in scan.get("Items", []):
                if "id" in it:
                    batch.delete_item(Key={"id": it["id"]})
    except Exception:
        # si falla el borrado, solo continuamos (no fatal)
        pass

    # guardar los nuevos (añadir id)
    i = 1
    for r in rows:
        item = dict(r)  # copia
        item["id"] = str(uuid.uuid4())
        item["#"] = i
        # DynamoDB necesita tipos simples (strings, numbers, lists, dicts)
        table.put_item(Item=item)
        i += 1

    return {"statusCode": 200, "body": json.dumps(rows, ensure_ascii=False)}
