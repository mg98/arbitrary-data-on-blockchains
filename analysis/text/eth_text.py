from .text import TextAnalysis
import codecs
from google.cloud import bigquery

class EthTextAnalysis(TextAnalysis):
	"""Text Analysis for the Ethereum blockchain."""
	
	def __init__(self, limit: int = 0):
		super().__init__('eth', limit)

	def run_core(self):
		"""Runs the query on BigQuery and persists results to the database."""

		query = """
			DECLARE REGEX_UTF8 DEFAULT "^0x{regex_utf8}+$";

			SELECT `hash`, `input` AS `data`, ROUND(IEEE_DIVIDE(`value`, 1000000000)) AS gwei_value, t.`block_timestamp`, 'tx' AS `type`,
			CASE WHEN c.`address` IS NOT NULL THEN true ELSE false END AS to_contract
			FROM `bigquery-public-data.crypto_ethereum.transactions` t
			LEFT OUTER JOIN `bigquery-public-data.crypto_ethereum.contracts` c
			ON c.`address` = `to_address`
			WHERE REGEXP_CONTAINS(`input`, REGEX_UTF8)

			# UNION ALL

			# SELECT `hash`, `extra_data` AS `data`, 0 AS gwei_value, `timestamp` AS `block_timestamp`, 'coinbase' AS `type`,
			# false AS to_contract
			# FROM `bigquery-public-data.crypto_ethereum.blocks`
			# WHERE REGEXP_CONTAINS(`extra_data`, REGEX_UTF8)

			{limit}
		""".format(
			regex_utf8=TextAnalysis.REGEX_UTF8,
			limit='LIMIT {}'.format(self.limit) if self.limit is not None else ''
		)

		client = bigquery.Client()
		query_job = client.query(query)

		print("Writing results to db...")

		try:
			for tx in query_job:
				print(tx["hash"])

				hex_value = tx["data"][2:]
				if hex_value and not len(hex_value) % 2:
					self.insert(
						tx['hash'],
						codecs.decode(hex_value, 'hex'),
						int(tx['gwei_value']),
						tx['block_timestamp'].strftime('%Y-%m-%d %H:%M:%S'),
						tx['type'],
						tx['to_contract']
					)
			print("Success!")
		except Exception as e:
			print("Something went really wrong!")
			print(e)
