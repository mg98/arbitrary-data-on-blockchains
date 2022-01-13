from .files import FilesAnalysis
from google.cloud import bigquery
import sys

class BtcFilesAnalysis(FilesAnalysis):
	"""Files Analysis for the Bitcoin blockchain."""

	"""Identifier of analyzed blockchain."""
	CHAIN = 'btc'

	def run_core(self):
		"""Runs the query on BigQuery and persists results to the database."""

		sigs = [v for values in self.file_signatures.values() for v in values]

		#sql_data_script = "ARRAY_TO_STRING(REGEXP_EXTRACT_ALL(`script_asm`, '[a-f0-9]{40,}'), '')"

		data = "STRING_AGG(ARRAY_TO_STRING(REGEXP_EXTRACT_ALL(`script_asm`, r'[a-f0-9]{40,}'), ''), '')"
		
		query = """
			-- Non-Standard OP_RETURN
			SELECT `hash`, `block_timestamp`, `output_value` AS `value`, (
				SELECT AS STRUCT 
					STRING_AGG(`type`) AS `type`, 
					{data} AS `data` 
				FROM t.`outputs` o
			) AS `outputs`
			FROM `bigquery-public-data.crypto_bitcoin.transactions` t
			WHERE {script_has_sig}

			{limit}
		""".format(
			script_has_sig=' OR '.join(list(map(lambda fs: f"(SELECT {data} FROM t.`outputs`) LIKE '%{fs}%'", sigs))),
			data=data,
			limit=f'LIMIT {self.limit}' if self.limit is not None else ''
		)

		print(query)

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
				print(tx)
				# candidate with earliest occurrence of signature wins (mime type, sig start pos, value)
				winner = (None, 0, None)
				for mime_type, sigs in self.file_signatures.items():
					for sig in sigs:
						sig_start = tx['outputs']['data'].find(sig)
						if sig_start != -1 and sig_start > winner[1]:
							winner = (mime_type, sig_start, tx['outputs']['data'][sig_start:])
							if sig_start == 0: break

				hex_value = winner[2]
				
				if hex_value and not len(hex_value) % 2:
					insert(
						tx['hash'],
						winner[0],
						'Embedded',
						tx['block_timestamp'],
						tx['outputs']['type'],
						FilesAnalysis.hex_to_base64(hex_value)
					)
				else:
					print("Failed to decode input.", hex_value)
			print("Success!")
		except Exception as e:
			print("Something went really wrong!")
			print(e)
