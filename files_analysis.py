from google.cloud import bigquery
import codecs
import sqlite3

FILE_SIGNATURES = {
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
    "video/avi": ["52494646"],
    "archive/zip": ["504B0304"],
    "archive/rar": ["526172211A0700", "526172211A070100"],
    "archive/7zip": ["377ABCAF271C"],
    "archive/tar": ["7573746172003030", "7573746172202000"],
    "archive/targz": ["1f8b"]
}

def expensive_type(mime_type: str) -> bool:
    return mime_type.startswith("audio") or mime_type.startswith("video") or mime_type.startswith("archive")

signatures = [v for values in FILE_SIGNATURES.values() for v in values]
signatures_for_embedded = [v for mime_type in FILE_SIGNATURES.keys() for v in FILE_SIGNATURES[mime_type] if not expensive_type(mime_type)]
sql_likes = list(map(lambda fs: "`input` LIKE '0x{}%'".format(fs), signatures)) + list(map(lambda fs: "`input` LIKE '0x%{}%'".format(fs), signatures_for_embedded))

def run(limit):
	query = """
		SELECT `hash`, `input`
		FROM `bigquery-public-data.crypto_ethereum.transactions`
		WHERE {} {}
	""".format(' OR '.join(sql_likes), 'LIMIT {}'.format(limit) if limit is not None else '')

	client = bigquery.Client()
	query_job = client.query(query)

	print("Writing results to db...")

	conn = sqlite3.connect("results.db")
	cursor = conn.cursor()
	cursor.execute("DROP TABLE IF EXISTS files_results")
	cursor.execute("CREATE TABLE files_results(hash TEXT, mime_type TEXT, method TEXT, data TEXT)")
	conn.commit()

	def insert(hash, mime_type, method, data):
		cursor.execute("INSERT INTO files_results VALUES(?, ?, ?, ?)", (hash, mime_type, method, data))
		conn.commit()

	def get_mime_type(input):
		for (mime_type, sigs) in FILE_SIGNATURES.items():
			for sig in sigs:
				if sig in input: return mime_type

	def hex_to_base64(hex_value):
		return codecs.encode(codecs.decode(hex_value, 'hex'), 'base64').decode()

	try:
		for tx in query_job:
			print(tx["hash"])

			mime_type = get_mime_type(tx['input'])

			for sig in FILE_SIGNATURES[mime_type]:
				start = tx["input"].find(sig)
				if start != -1 and (start == 2 or not (mime_type.startswith("audio") or mime_type.startswith("video"))):
					hex_value = tx["input"][start:]
					break

			if hex_value and not len(hex_value) % 2:
				insert(tx['hash'], mime_type, "Embedded" if start == 2 else "Injected", hex_to_base64(hex_value))
			else:
				print("Failed to decode input.")
	except Exception as e:
		print("Something went really wrong!")
		print(e)
	finally:
		conn.close()
