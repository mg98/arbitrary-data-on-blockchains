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
			DECLARE REGEX_UTF8 DEFAULT "{regex_utf8}";

			-- standard contain >= 90% utf8
			SELECT `hash`, `block_timestamp`, `output_value` AS `value`,
			(SELECT AS STRUCT  
				STRING_AGG(DISTINCT `type`) AS `type`,
				ARRAY_TO_STRING(REGEXP_EXTRACT_ALL(STRING_AGG(`script_asm`, ''), REGEX_UTF8), '') AS `data`
				FROM t.`outputs` o WHERE o.`type` != 'nonstandard') AS `outputs`
			FROM `bigquery-public-data.crypto_bitcoin.transactions` t
			WHERE 
			(SELECT ARRAY_TO_STRING(REGEXP_EXTRACT_ALL(STRING_AGG(`script_asm`, ''), REGEX_UTF8), '') FROM t.`outputs` o WHERE o.`type` != 'nonstandard') != ''
			AND LENGTH(ARRAY_TO_STRING(REGEXP_EXTRACT_ALL(
				(SELECT STRING_AGG(`script_asm`, '') FROM t.`outputs` o WHERE o.`type` != 'nonstandard'), 
			REGEX_UTF8), '')) >= CAST(LENGTH(
				(SELECT REGEXP_REPLACE(STRING_AGG(`script_asm`, ''), r'OP_[A-Z0-9]*|\ ', '') FROM t.`outputs` o WHERE o.`type` != 'nonstandard')
			) * 0.9 AS INT64)
			AND {outputs_unspent}

			UNION ALL

			-- nonstandard (ex. op_return) contain >= 90% utf8
			SELECT `hash`, `block_timestamp`, `output_value` AS `value`,
			(SELECT AS STRUCT 
				'nonstandard output' AS `type`, 
				ARRAY_TO_STRING(REGEXP_EXTRACT_ALL(STRING_AGG(`script_asm`, ''), REGEX_UTF8), '') AS `data`
				FROM t.`outputs` o WHERE o.`type` = 'nonstandard' AND o.`script_asm` NOT LIKE 'OP_RETURN %') AS `outputs`
			FROM `bigquery-public-data.crypto_bitcoin.transactions` t
			WHERE 
			(SELECT ARRAY_TO_STRING(REGEXP_EXTRACT_ALL(STRING_AGG(`script_asm`, ''), REGEX_UTF8), '') FROM t.`outputs` o WHERE o.`type` = 'nonstandard' AND o.`script_asm` NOT LIKE 'OP_RETURN %') != ''
			AND LENGTH(ARRAY_TO_STRING(REGEXP_EXTRACT_ALL(
				(SELECT STRING_AGG(`script_asm`, '') FROM t.`outputs` o WHERE o.`type` = 'nonstandard' AND o.`script_asm` NOT LIKE 'OP_RETURN %'), 
			REGEX_UTF8), '')) >= CAST(LENGTH(
				(SELECT REGEXP_REPLACE(STRING_AGG(`script_asm`, ''), r'OP_[A-Z0-9]*|\ ', '') FROM t.`outputs` o WHERE o.`type` = 'nonstandard' AND o.`script_asm` NOT LIKE 'OP_RETURN %')
			) * 0.9 AS INT64)
			AND {outputs_unspent}

			UNION ALL

			-- nonstandard inputs (ex. op_return) contain >= 90% utf8
			SELECT `hash`, `block_timestamp`, `output_value` AS `value`, 
			(SELECT AS STRUCT 
				'nonstandard input' AS `type`, 
				ARRAY_TO_STRING(REGEXP_EXTRACT_ALL(STRING_AGG(REPLACE(`script_asm`, '[ALL]', ''), ''), REGEX_UTF8), '') AS `data` 
				FROM t.`inputs` i WHERE i.`type` = 'nonstandard') AS `outputs`
			FROM `bigquery-public-data.crypto_bitcoin.transactions` t
			WHERE 
			(SELECT ARRAY_TO_STRING(REGEXP_EXTRACT_ALL(STRING_AGG(REPLACE(`script_asm`, '[ALL]', ''), ''), REGEX_UTF8), '') FROM t.`inputs` o WHERE o.`type` = 'nonstandard') != ''
			AND LENGTH(ARRAY_TO_STRING(REGEXP_EXTRACT_ALL(
				(SELECT STRING_AGG(REPLACE(`script_asm`, '[ALL]', ''), '') FROM t.`inputs` i WHERE i.`type` = 'nonstandard'), 
			REGEX_UTF8), '')) >= CAST(LENGTH(
				(SELECT STRING_AGG(REPLACE(`script_asm`, '[ALL]', ''), '') FROM t.`inputs` i WHERE i.`type` = 'nonstandard')
			) * 0.9 AS INT64)
			AND {outputs_unspent}

			UNION ALL

			-- scripthash inputs contain >= 90% utf8
			SELECT `hash`, `block_timestamp`, `output_value` AS `value`, 
			(SELECT AS STRUCT 
				'scripthash input' AS `type`, 
				ARRAY_TO_STRING(REGEXP_EXTRACT_ALL(STRING_AGG(REPLACE(`script_asm`, '[ALL]', ''), ''), REGEX_UTF8), '') AS `data` 
				FROM t.`inputs` i WHERE i.`type` = 'scripthash') AS `outputs`
			FROM `bigquery-public-data.crypto_bitcoin.transactions` t
			WHERE 
			(SELECT ARRAY_TO_STRING(REGEXP_EXTRACT_ALL(STRING_AGG(REPLACE(`script_asm`, '[ALL]', ''), ''), REGEX_UTF8), '') FROM t.`inputs` o WHERE o.`type` = 'scripthash') != ''
			AND LENGTH(ARRAY_TO_STRING(REGEXP_EXTRACT_ALL(
				(SELECT STRING_AGG(REPLACE(`script_asm`, '[ALL]', ''), '') FROM t.`inputs` i WHERE i.`type` = 'scripthash'), 
			REGEX_UTF8), '')) >= CAST(LENGTH(
				(SELECT STRING_AGG(REPLACE(`script_asm`, '[ALL]', ''), '') FROM t.`inputs` i WHERE i.`type` = 'scripthash')
			) * 0.9 AS INT64)
			AND {outputs_unspent}

			UNION ALL

			-- nonstandard op_return
			SELECT `hash`, `block_timestamp`, `output_value` AS `value`, (
				SELECT AS STRUCT 
					'op_return' AS `type`, 
					ARRAY_TO_STRING(REGEXP_EXTRACT_ALL(STRING_AGG(SUBSTR(`script_asm`, 10), ''), REGEX_UTF8), '') AS `data` 
				FROM t.`outputs` o 
				WHERE o.`type` = 'nonstandard' AND o.`script_asm` LIKE 'OP_RETURN %'
			) AS `outputs`
			FROM `bigquery-public-data.crypto_bitcoin.transactions` t
			WHERE 
			(SELECT ARRAY_TO_STRING(REGEXP_EXTRACT_ALL(STRING_AGG(SUBSTR(`script_asm`, 10), ''), REGEX_UTF8), '') FROM t.`outputs` o WHERE o.`type` = 'nonstandard' AND o.`script_asm` LIKE 'OP_RETURN %') != ''
			AND {outputs_unspent}

			{limit}
		""".format(
			regex_utf8=TextAnalysis.REGEX_UTF8,
			limit='LIMIT {}'.format(self.limit) if self.limit is not None else '',
			outputs_unspent='NOT EXISTS(SELECT 1 FROM `bigquery-public-data.crypto_bitcoin.inputs` i WHERE i.`spent_transaction_hash` = t.`hash`)'
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
				hex_value = tx['outputs']['data']
				if hex_value and not len(hex_value) % 2:
					insert(
						tx['hash'],
						codecs.decode(hex_value, 'hex'),
						int(tx['value']),
						tx['block_timestamp'].strftime('%Y-%m-%d %H:%M:%S'),
						tx['outputs']['type']
					)
				else:
					print('******** NOT INSERTED', tx, hex_value)
			print("Success!")
		except Exception as e:
			print("Something went really wrong!")
			print(e)
