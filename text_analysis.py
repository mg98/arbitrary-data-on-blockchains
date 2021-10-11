from google.cloud import bigquery
import codecs
import sqlite3

REGEX_UTF8 = "(20|21|22|23|24|25|26|27|28|29|2a|2b|2c|2d|2e|2f|30|31|32|33|34|35|36|37|38|39|3a|3b|3c|3d|3e|3f|40|41|42|43|44|45|46|47|48|49|4a|4b|4c|4d|4e|4f|50|51|52|53|54|55|56|57|58|59|5a|5b|5c|5d|5e|5f|60|61|62|63|64|65|66|67|68|69|6a|6b|6c|6d|6e|6f|70|71|72|73|74|75|76|77|78|79|7a|7b|7c|7d|7e|c2a0|c2a1|c2a2|c2a3|c2a4|c2a5|c2a6|c2a7|c2a8|c2a9|c2aa|c2ab|c2ac|c2ad|c2ae|c2af|c2b0|c2b1|c2b2|c2b3|c2b4|c2b5|c2b6|c2b7|c2b8|c2b9|c2ba|c2bb|c2bc|c2bd|c2be|c2bf|c380|c381|c382|c383|c384|c385|c386|c387|c388|c389|c38a|c38b|c38c|c38d|c38e|c38f|c390|c391|c392|c393|c394|c395|c396|c397|c398|c399|c39a|c39b|c39c|c39d|c39e|c39f|c3a0|c3a1|c3a2|c3a3|c3a4|c3a5|c3a6|c3a7|c3a8|c3a9|c3aa|c3ab|c3ac|c3ad|c3ae|c3af|c3b0|c3b1|c3b2|c3b3|c3b4|c3b5|c3b6|c3b7|c3b8|c3b9|c3ba|c3bb|c3bc|c3bd|c3be|c3bf)"

def run(limit):
	query = """
		SELECT `hash`, `input`
		FROM `bigquery-public-data.crypto_ethereum.transactions`
		WHERE LENGTH(`input`) > 10 AND REGEXP_CONTAINS(`input`, r'^0x{}*$')
		{}
	""".format(REGEX_UTF8, 'LIMIT {}'.format(limit) if limit is not None else '')

	client = bigquery.Client()
	query_job = client.query(query)

	print("Writing results to db...")

	conn = sqlite3.connect("results.db")
	cursor = conn.cursor()
	cursor.execute("DROP TABLE IF EXISTS text_results")
	cursor.execute("CREATE TABLE text_results(hash TEXT, data TEXT)")
	conn.commit()

	def insert(hash, data):
		cursor.execute("INSERT INTO text_results VALUES (?, ?)", (hash, data))
		conn.commit()

	try:
		for tx in query_job:
			print(tx["hash"])

			hex_value = tx["input"][2:]
			if hex_value and not len(hex_value) % 2:
				data = codecs.decode(hex_value, "hex")
				insert(tx['hash'], data)
	except Exception as e:
		print("Something went really wrong!")
		print(e)
	finally:
		conn.close()
