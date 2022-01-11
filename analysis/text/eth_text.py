from .text import TextAnalysis
import codecs
from google.cloud import bigquery

class EthTextAnalysis(TextAnalysis):
	"""Text Analysis for the Ethereum blockchain."""

	"""Identifier of analyzed blockchain."""
	CHAIN = 'eth'

	def run_core(self):
		"""Runs the query on BigQuery and persists results to the database."""

		query = """
			SELECT `hash`, `input`, ROUND(IEEE_DIVIDE(`value`, 1000000000)) AS gwei_value, t.`block_timestamp`, CASE WHEN c.`address` IS NOT NULL THEN true ELSE false END AS to_contract
			FROM `bigquery-public-data.crypto_ethereum.transactions` t
			LEFT OUTER JOIN `bigquery-public-data.crypto_ethereum.contracts` c
			ON c.`address` = `to_address`
			WHERE REGEXP_CONTAINS(`input`, r'^0x{}+$') {}
		""".format(
			TextAnalysis.REGEX_UTF8,
			'LIMIT {}'.format(self.limit) if self.limit is not None else ''
		)

		client = bigquery.Client()
		query_job = client.query(query)

		print("Writing results to db...")

		def insert(hash: str, data: str, gwei_value: int, block_timestamp: str, to_contract: bool):
			self.conn.execute("""
				INSERT INTO text_results (
					chain, hash, data, value, block_timestamp, to_contract
				) VALUES (?, ?, ?, ?, ?, ?)
			""", (EthTextAnalysis.CHAIN, hash, data, gwei_value, block_timestamp, to_contract))
			self.conn.commit()

		try:
			for tx in query_job:
				print(tx["hash"])
				hex_value = tx["input"][2:]
				print(tx["input"])
				if hex_value and not len(hex_value) % 2:
					insert(
						tx['hash'],
						codecs.decode(hex_value, 'hex'),
						int(tx['gwei_value']),
						tx['block_timestamp'].strftime('%Y-%m-%d %H:%M:%S'),
						tx['to_contract']
					)
			print("Success!")
		except Exception as e:
			print("Something went really wrong!")
			print(e)
