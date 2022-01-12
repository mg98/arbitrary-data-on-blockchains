from .files import FilesAnalysis
from google.cloud import bigquery
import sys

class BtcFilesAnalysis(FilesAnalysis):
	"""Files Analysis for the Bitcoin blockchain."""

	"""Identifier of analyzed blockchain."""
	CHAIN = 'btc'

	def run_core(self):
		"""Runs the query on BigQuery and persists results to the database."""

		# concat all hexadecimal segments with at least 40 characters
		"ARRAY_TO_STRING(REGEXP_EXTRACT_ALL(`script_asm`, '[a-f0-9]{40,}'), '')"

		sigs = [v for values in self.file_signatures.values() for v in values]

		sql_data_script = "ARRAY_TO_STRING(REGEXP_EXTRACT_ALL(`script_asm`, '[a-f0-9]{40,}'), '')"
		
		query = """
			SELECT `transaction_hash` AS `hash`, `block_timestamp`, CONCAT(`type`, ' output script') AS `type`, (
				SELECT AS STRUCT SUM(`value`) AS `value`, STRING_AGG({sql_data_script}, '') AS `data`
					FROM `bigquery-public-data.crypto_bitcoin.outputs` o
					WHERE o.`transaction_hash` = big_o.`transaction_hash`
				) AS `outputs`
			FROM `bigquery-public-data.crypto_bitcoin.outputs` AS big_o
			WHERE {script_has_sig} AND {output_unspent}

			{limit}
		""".format(
			sql_data_script=sql_data_script,

			script_has_sig=' OR '.join(list(map(lambda fs: "{} LIKE '%{}%'".format(
					# concat all hexadecimal segments with at least 40 characters
					sql_data_script, fs
				), sigs))),

			address_is_hex="""
				LENGTH(ARRAY_TO_STRING(REGEXP_EXTRACT_ALL(
					REPLACE(ARRAY_TO_STRING(o.`addresses`, ''), 'nonstandard', ''),
					'[a-f0-9]'
				), '')) = LENGTH(REPLACE(ARRAY_TO_STRING(o.`addresses`, ''), 'nonstandard', ''))
			""",

			address_has_sig=' OR '.join(list(map(lambda fs: "{} LIKE '%{}%'".format(
					# concat output addresses
					"ARRAY_TO_STRING(`addresses`, '')", fs
				), sigs))),

			output_unspent="""
				NOT EXISTS(
					SELECT 1 FROM `bigquery-public-data.crypto_bitcoin.inputs` i
					WHERE i.`transaction_hash` = big_o.`transaction_hash`
				)
			""",
			
			limit='LIMIT {}'.format(self.limit) if self.limit is not None else ''
		)

		client = bigquery.Client()
		query_job = client.query(query)

		print("Writing results to db...")

		def insert(hash: str, mime_type: str, method: str, block_timestamp: str, type: str, data: str):
			self.conn.execute("""
				INSERT INTO files_results (
					chain, hash, mime_type, method, block_timestamp, type, data
				) VALUES (?, ?, ?, ?, ?, ?, ?)
			""", (BtcFilesAnalysis.CHAIN, hash, mime_type, method, block_timestamp, type, data))
			self.conn.commit()

		try:
			for tx in query_job:
				print(tx['hash'])
				
				print(tx['type'], tx['outputs']['data'])

				# candidate with longest signature wins (mime type, sig length, value)
				winner = (None, 0, None)
				for mime_type, sigs in self.file_signatures.items():
					for sig in sigs:
						sig_start = tx['outputs']['data'].find(sig)
						if sig_start != -1 and len(sig) > winner[1]:
							winner = (mime_type, len(sig), tx['outputs']['data'][sig_start:])

				hex_value = winner[2]
				
				if hex_value and not len(hex_value) % 2:
					insert(
						tx['hash'],
						winner[0],
						'Embedded',
						tx['block_timestamp'],
						tx['type'],
						FilesAnalysis.hex_to_base64(hex_value)
					)
				else:
					print("Failed to decode input.", hex_value)
			print("Success!")
		except Exception as e:
			print("Something went really wrong!")
			print(e)
