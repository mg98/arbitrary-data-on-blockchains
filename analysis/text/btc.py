from .text import TextAnalysis
import codecs
from google.cloud import bigquery

class BtcTextAnalysis(TextAnalysis):
	"""Text Analysis for the Bitcoin blockchain."""

	"""Identifier of analyzed blockchain."""
	CHAIN = 'btc'

	def run_core(self):
		"""Runs the query on BigQuery and persists results to the database."""

		query = """
			SELECT `transaction_hash` as `hash`, `block_timestamp`, `type`, `value`, 
				ARRAY_TO_STRING(REGEXP_EXTRACT_ALL(SUBSTR(`script_asm`, 10), '{regex_utf8}'), '') as `data`
			FROM `bigquery-public-data`.`crypto_bitcoin`.`outputs`
			WHERE `script_asm` LIKE 'OP_RETURN %' {ts_clause}
			UNION DISTINCT
			SELECT 
				`transaction_hash` as `hash`, `block_timestamp`, `type`, `value`,
				ARRAY_TO_STRING(REGEXP_EXTRACT_ALL(`script_asm`, '{regex_utf8}'), '') AS `data`
			FROM `bigquery-public-data`.`crypto_bitcoin`.`outputs`
			WHERE `script_asm` NOT LIKE 'OP_RETURN %'
			AND LENGTH(ARRAY_TO_STRING(REGEXP_EXTRACT_ALL(`script_asm`, '{regex_utf8}'), '')) >= CAST(LENGTH(`script_asm`) * 0.9 AS INT64)
			{limit}
		""".format(
			regex_utf8=TextAnalysis.REGEX_UTF8,
			limit='LIMIT {}'.format(self.limit) if self.limit is not None else ''
		)

		client = bigquery.Client()
		query_job = client.query(query)

		print("Writing results to db...")

		def insert(hash: str, data: str, value: int, block_timestamp: str, type: str):
			self.conn.execute("""
				INSERT INTO text_results (
					chain, hash, data, value, block_timestamp, type
				) VALUES (?, ?, ?, ?, ?, ?)
			""", (BtcTextAnalysis.CHAIN, hash, data, value, block_timestamp, type))
			self.conn.commit()

		try:
			for tx in query_job:
				print(tx['hash'])
				hex_value = tx["data"]
				print(tx)
				if hex_value and not len(hex_value) % 2:
					insert(
						tx['hash'],
						codecs.decode(hex_value, 'hex'),
						int(tx['value']),
						tx['block_timestamp'].strftime('%Y-%m-%d %H:%M:%S'),
						tx['type']
					)
			print("Success!")
		except Exception as e:
			print("Something went really wrong!")
			print(e)
