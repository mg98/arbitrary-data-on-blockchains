from .files import FilesAnalysis
from google.cloud import bigquery

class EthFilesAnalysis(FilesAnalysis):
	"""Files Analysis for the Ethereum blockchain."""

	def __init__(self, limit: int = 0, reset: bool = False, mime_types: list[str] = ['*']):
		super().__init__('eth', limit, reset, mime_types)

	def run_core(self):
		"""Runs the query on BigQuery and persists results to the database."""
		
		sigs = [v for values in self.file_signatures.values() for v in values]
		sql_likes = lambda field: list(map(lambda sig: f"`{field}`" + " LIKE '0x" + ('%' if len(sig) >= 6 else '') + sig + "%'", sigs))

		query = """
			SELECT `hash`, `input` AS `data`, t.`block_timestamp`, 'tx' AS `type`,
			CASE WHEN c.`address` IS NOT NULL THEN true ELSE false END AS to_contract
			FROM `bigquery-public-data.crypto_ethereum.transactions` t
			LEFT OUTER JOIN `bigquery-public-data.crypto_ethereum.contracts` c
			ON c.`address` = `to_address`
			WHERE {tx_where}

			UNION ALL

			SELECT `hash`, `extra_data` AS `data`, `timestamp` AS `block_timestamp`, 'coinbase' AS `type`,
			false AS to_contract
			FROM `bigquery-public-data.crypto_ethereum.blocks`
			WHERE {coinbase_where}

			{limit}
		""".format(
			tx_where=' OR '.join(sql_likes('input')),
			coinbase_where=' OR '.join(sql_likes('extra_data')),
			limit='LIMIT {}'.format(self.limit) if self.limit is not None else ''
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
						sig_start = tx['data'].find(sig)
						if sig_start != -1 and sig_start > winner[1]:
							winner = (mime_type, sig_start, tx['data'][sig_start:])
							if sig_start == 2: break

				mime_type, sig_start, hex_value = winner

				if hex_value and not len(hex_value) % 2:
					self.insert(
						tx['hash'],
						mime_type,
						'Embedded' if sig_start == 2 else 'Injected',
						tx['block_timestamp'].strftime('%Y-%m-%d %H:%M:%S'),
						tx['type'],
						FilesAnalysis.hex_to_base64(hex_value),
						tx['to_contract']
					)
				else:
					print("Failed to decode input.")
			print("Success!")
		except Exception as e:
			print("Something went really wrong!")
			print(e)
