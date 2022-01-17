from .files import FilesAnalysis
from google.cloud import bigquery
import sys

class BtcFilesAnalysis(FilesAnalysis):
	"""Files Analysis for the Bitcoin blockchain."""

	"""Identifier of analyzed blockchain."""
	CHAIN = 'btc'

	def __init__(self, limit: int = 0, reset: bool = False, mime_types: list[str] = ['*']):
		super().__init__('btc', limit, reset, mime_types)

	def run_core(self):
		"""Runs the query on BigQuery and persists results to the database."""

		sigs = [v for values in self.file_signatures.values() for v in values]

		data = "STRING_AGG(ARRAY_TO_STRING(REGEXP_EXTRACT_ALL(`script_asm`, r'[a-f0-9]{40,}'), ''), '')"
		coinbase_data = "ARRAY_TO_STRING(REGEXP_EXTRACT_ALL(SUBSTR(`coinbase_param`, 17), r'[a-f0-9]{40,}'), '')"

		like = lambda sig: ('%' if len(sig) >= 6 else '') + sig + '%'

		query = """
			-- Output Scripts
			SELECT `hash`, `block_timestamp`, `output_value` AS `value`, (
				SELECT AS STRUCT 
					STRING_AGG(DISTINCT `type`) AS `type`, 
					{data} AS `data` 
				FROM t.`outputs` o
			) AS `outputs`
			FROM `bigquery-public-data.crypto_bitcoin.transactions` t
			WHERE {output_has_sig}

			UNION ALL

			-- Non-Standard Input Scripts
			SELECT `hash`, `block_timestamp`, `output_value` AS `value`, (
				SELECT AS STRUCT 
					CONCAT('input ', STRING_AGG(DISTINCT `type`)) AS `type`, 
					{data} AS `data` 
				FROM t.`inputs` i
				WHERE i.`type` = 'nonstandard'
			) AS `outputs`
			FROM `bigquery-public-data.crypto_bitcoin.transactions` t
			WHERE {nonst_input_has_sig}

			UNION ALL

			-- ScriptHash (P2SH) Inputs
			SELECT `hash`, `block_timestamp`, `output_value` AS `value`, (
				SELECT AS STRUCT 
					CONCAT('input ', STRING_AGG(DISTINCT `type`)) AS `type`, 
					{data} AS `data` 
				FROM t.`inputs` i
				WHERE i.`type` = 'scripthash'
			) AS `outputs`
			FROM `bigquery-public-data.crypto_bitcoin.transactions` t
			WHERE {p2sh_input_has_sig}

			UNION ALL

			-- Coinbase Inputs
			SELECT `hash`, `timestamp` AS `block_timestamp`, 0 as `value`, STRUCT(
					'coinbase' AS `type`, 
					{coinbase_data} AS `data` 
			) AS `outputs`
			FROM `bigquery-public-data.crypto_bitcoin.blocks` t
			WHERE {coinbase_script_has_sig}

			{limit}
		""".format(
			data=data,
			coinbase_data=coinbase_data,
			output_has_sig=' OR '.join(list(map(lambda fs: f"(SELECT {data} FROM t.`outputs`) LIKE '{like(fs)}'", sigs))),
			nonst_input_has_sig=' OR '.join(list(map(lambda fs: f"(SELECT {data} FROM t.`inputs` i WHERE i.`type` = 'nonstandard') LIKE '{like(fs)}'", sigs))),
			p2sh_input_has_sig=' OR '.join(list(map(lambda fs: f"(SELECT {data} FROM t.`inputs` i WHERE i.`type` = 'scripthash') LIKE '{like(fs)}'", sigs))),
			coinbase_script_has_sig=' OR '.join(list(map(lambda fs: f"{coinbase_data} LIKE '{like(fs)}'", sigs))),
			limit=f'LIMIT {self.limit}' if self.limit is not None else ''
		)

		client = bigquery.Client()
		query_job = client.query(query)

		print("Writing results to db...")

		try:
			for tx in query_job:
				print(tx['hash'])

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
					self.insert(
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
