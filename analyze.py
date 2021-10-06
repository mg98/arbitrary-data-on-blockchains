from google.cloud import bigquery
import codecs
import sqlite3
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('--limit', help='Limit the results processed by the BigQuery SQL query. If not set, proceeds to query the entire blockchain.')
args = parser.parse_args()

file_signatures = {
    "image/png": ["89504e470d0a1a0a"],
    "image/jpeg": ["ffd8ff"],
    "image/gif": ["474946383761", "474946383961"],
    "image/webp": ["52494646"],
    "application/pdf": ["255044462d"],
    "audio/mp3": ["494433"],
    "audio/wav": ["52494646"],
    "video/mp4": ["667479"],
    "video/mov": ["6674797071742020"],
    "video/wmv": ["a6d900aa0062ce6c"],
    "video/avi": ["52494646"]
}

def audio_or_video(mime_type: str) -> bool:
    return mime_type.startswith("audio") or mime_type.startswith("video")

signatures = [v for values in file_signatures.values() for v in values]
non_av_signatures = [v for mime_type in file_signatures.keys() for v in file_signatures[mime_type] if not audio_or_video(mime_type)]
sql_likes = list(map(lambda fs: "`input` LIKE '0x{}%'".format(fs), signatures)) + list(map(lambda fs: "`input` LIKE '0x%{}%'".format(fs), non_av_signatures))

query = """
    SELECT `hash`, `input`
    FROM `bigquery-public-data.crypto_ethereum.transactions`
    WHERE {} {}
""".format(' OR '.join(sql_likes), 'LIMIT {}'.format(args.limit) if args.limit is not None else '')

client = bigquery.Client()
query_job = client.query(query)

print("Writing results to db...")

conn = sqlite3.connect("results.db")
cursor = conn.cursor()
cursor.execute("DROP TABLE results")
cursor.execute("CREATE TABLE results(hash TEXT, mime_type TEXT, method TEXT, data TEXT)")
conn.commit()

def insert(hash, mime_type, method, data):
    cursor.execute("INSERT INTO results VALUES('{}', '{}', '{}', '{}')".format(hash, mime_type, method, data))
    conn.commit()

def get_mime_type(input):
    for (mime_type, sigs) in file_signatures.items():
        for sig in sigs:
            if sig in input: return mime_type

def hex_to_base64(hex_value, mime_type):
    return codecs.encode(codecs.decode(hex_value, 'hex'), 'base64').decode()

try:
    for tx in query_job:
        print(tx["hash"])

        mime_type = get_mime_type(tx['input'])

        for sig in file_signatures[mime_type]:
            start = tx["input"].find(sig)
            if start != -1 and (start == 2 or not (mime_type.startswith("audio") or mime_type.startswith("video"))):
                hex_value = tx["input"][start:]
                break

        if hex_value and not len(hex_value) % 2:
            insert(tx['hash'], mime_type, "Embedded" if start == 2 else "Injected", hex_to_base64(hex_value, mime_type))
        else:
            print("Failed to decode input.")
except Exception as e:
    print("Something went really wrong!")
    print(e)
finally:
    conn.close()
