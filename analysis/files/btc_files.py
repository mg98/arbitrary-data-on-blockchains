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

		data = "ARRAY_TO_STRING(REGEXP_EXTRACT_ALL(`script_asm`, r'[a-f0-9]{40,}'), '')"
		
		def like(sig):
			return ('%' if len(sig) > 6 else '') + sig + '%'

		query = """
			-- Output Scripts
			SELECT `hash`, `block_timestamp`, `output_value` AS `value`, (
				SELECT AS STRUCT 
					STRING_AGG(DISTINCT `type`) AS `type`, 
					{data} AS `data` 
				FROM t.`inputs` o
			) AS `outputs`
			FROM `bigquery-public-data.crypto_bitcoin.transactions` t
			WHERE {output_script_has_sig}

			UNION ALL

			-- Input Scripts
			SELECT `hash`, `block_timestamp`, `output_value` AS `value`, (
				SELECT AS STRUCT 
					CONCAT('input ', STRING_AGG(DISTINCT `type`) AS `type`, 
					{data} AS `data` 
				FROM t.`inputs`
			) AS `outputs`
			FROM `bigquery-public-data.crypto_bitcoin.transactions` t
			WHERE {input_script_has_sig}

			UNION ALL

			-- Coinbase Inputs
			SELECT `hash`, `timestamp` AS `block_timestamp`, 0 as `value`, STRUCT(
					'coinbase' AS `type`, 
					{data} AS `data` 
			) AS `outputs`
			FROM `bigquery-public-data.crypto_bitcoin.blocks` t
			WHERE {coinbase_script_has_sig}

			{limit}
		""".format(
			data=data,
			output_script_has_sig=' OR '.join(list(map(lambda fs: f"(SELECT STRING_AGG({data}, '') FROM t.`outputs`) LIKE '{like(fs)}'", sigs))),
			input_script_has_sig=' OR '.join(list(map(lambda fs: f"(SELECT STRING_AGG({data}, '') FROM t.`inputs`) LIKE '{like(fs)}'", sigs))),
			coinbase_script_has_sig=' OR '.join(list(map(lambda fs: f"{data} LIKE '{like(fs)}'", sigs))),
			limit=f'LIMIT {self.limit}' if self.limit is not None else ''
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
				# Candidate with earliest occurrence of signature wins.
				# Tuple (mime type, sig start pos, value)
				winner = (None, -1, 0, None)
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
