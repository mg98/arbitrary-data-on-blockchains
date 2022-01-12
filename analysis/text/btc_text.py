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
			SELECT `hash`, `block_timestamp`, `output_value` AS `value`, (
				SELECT AS STRUCT  
					STRING_AGG(DISTINCT o.`type`) AS `type`,
					ARRAY_TO_STRING(REGEXP_EXTRACT_ALL(REGEXP_REPLACE(STRING_AGG(`script_asm`, ''), r'\ ?OP_[A-Z0-9]*\ ?', ''), REGEX_UTF8), '') AS `data`
				FROM t.`outputs` o 
				WHERE o.`type` != 'nonstandard'
			) AS `outputs`
			FROM `bigquery-public-data.crypto_bitcoin.transactions` t
			WHERE 
			IFNULL((
				SELECT ARRAY_TO_STRING(REGEXP_EXTRACT_ALL(REGEXP_REPLACE(STRING_AGG(`script_asm`, ''), r'\ ?OP_[A-Z0-9]*\ ?', ''), REGEX_UTF8), '') AS `data`
				FROM t.`outputs` o 
				WHERE o.`type` != 'nonstandard'
			), '') != ''
			AND LENGTH(ARRAY_TO_STRING(REGEXP_EXTRACT_ALL(
				(SELECT STRING_AGG(`script_asm`, '') FROM t.`outputs` o WHERE o.`type` != 'nonstandard'), 
			REGEX_UTF8), '')) >= CAST(LENGTH(
				(SELECT REGEXP_REPLACE(STRING_AGG(`script_asm`, ''), r'\ ?OP_[A-Z0-9]*\ ?', '') FROM t.`outputs` o WHERE o.`type` != 'nonstandard')
			) * 0.9 AS FLOAT64)

			UNION ALL

			-- Non-Standard Outputs (ex. OP_RETURN)
			SELECT `hash`, `block_timestamp`, `output_value` AS `value`,
			(SELECT AS STRUCT 
				'nonstandard output' AS `type`, 
				ARRAY_TO_STRING(REGEXP_EXTRACT_ALL(STRING_AGG(`script_asm`, ''), REGEX_UTF8), '') AS `data`
				FROM t.`outputs` o WHERE o.`type` = 'nonstandard' AND o.`script_asm` NOT LIKE 'OP_RETURN %') AS `outputs`
			FROM `bigquery-public-data.crypto_bitcoin.transactions` t
			WHERE 
			'nonstandard' IN (SELECT `type` FROM t.`outputs`)
			AND (SELECT STRING_AGG(`script_asm`, '') FROM t.`outputs` o WHERE o.`type` = 'nonstandard' AND o.`script_asm` NOT LIKE 'OP_RETURN %') != ''
			AND REGEXP_CONTAINS(
				REGEXP_REPLACE(
					(SELECT STRING_AGG(`script_asm`, '') FROM t.`outputs` o WHERE o.`type` = 'nonstandard' AND o.`script_asm` NOT LIKE 'OP_RETURN %'), 
					r'\ ?OP_[A-Z0-9]*\ ',
					''
				),
				{full_regex_utf8}
			)

			UNION ALL

			-- Non-Standard Inputs (ex. OP_RETURN)
			SELECT `hash`, `block_timestamp`, `output_value` AS `value`, 
			(SELECT AS STRUCT 
				'nonstandard input' AS `type`, 
				STRING_AGG(REPLACE(`script_asm`, '[ALL]', ''), '') AS `data` 
				FROM t.`inputs` i WHERE i.`type` = 'nonstandard') AS `outputs`
			FROM `bigquery-public-data.crypto_bitcoin.transactions` t
			WHERE 
			-- Check if input script after removal of '[ALL]' occurrences represents a UTF-8 string.
			'nonstandard' IN (SELECT `type` FROM t.`inputs`)
			AND (SELECT STRING_AGG(REPLACE(`script_asm`, '[ALL]', ''), '') FROM t.`inputs` i WHERE i.`type` = 'nonstandard') != ''
			AND REGEXP_CONTAINS((SELECT STRING_AGG(REPLACE(`script_asm`, '[ALL]', ''), '') FROM t.`inputs` i WHERE i.`type` = 'nonstandard'), {full_regex_utf8})

			UNION ALL

			-- ScriptHash (P2SH) Inputs
			SELECT `hash`, `block_timestamp`, `output_value` AS `value`, 
			(SELECT AS STRUCT 
			    'scripthash input' AS `type`, 
			    REGEXP_REPLACE(STRING_AGG(`script_asm`, ''), r'(\[ALL\])|\ ', '') AS `data` 
			    FROM t.`inputs` i WHERE i.`type` = 'scripthash') AS `outputs`
			FROM `bigquery-public-data.crypto_bitcoin.transactions` t
			WHERE
			'scripthash' IN (SELECT `type` FROM t.`inputs`)
			AND (SELECT REGEXP_REPLACE(STRING_AGG(`script_asm`, ''), r'(\[ALL\])|\ ', '') FROM t.`inputs` i WHERE i.`type` = 'scripthash') != ''
			AND REGEXP_CONTAINS((SELECT REGEXP_REPLACE(STRING_AGG(`script_asm`, ''), r'(\[ALL\])|\ ', '') FROM t.`inputs` i WHERE i.`type` = 'scripthash'), {full_regex_utf8})


			UNION ALL

			-- Non-Standard OP_RETURN
			SELECT `hash`, `block_timestamp`, `output_value` AS `value`, (
				SELECT AS STRUCT 
					'op_return' AS `type`, 
					STRING_AGG(SUBSTR(`script_asm`, 11), '') AS `data` 
				FROM t.`outputs` o 
				WHERE o.`type` = 'nonstandard' AND o.`script_asm` LIKE 'OP_RETURN %'
			) AS `outputs`
			FROM `bigquery-public-data.crypto_bitcoin.transactions` t
			WHERE 
			-- Skip 10 characters for "OP_RETURN " string, then check for empty values and UTF-8.
			'nonstandard' IN (SELECT `type` FROM t.`outputs`)
			AND (SELECT STRING_AGG(SUBSTR(`script_asm`, 11), '') FROM t.`outputs` o WHERE o.`type` = 'nonstandard' AND o.`script_asm` LIKE 'OP_RETURN %') != ''
			AND REGEXP_CONTAINS((SELECT STRING_AGG(SUBSTR(`script_asm`, 11), '') FROM t.`outputs` o WHERE o.`type` = 'nonstandard' AND o.`script_asm` LIKE 'OP_RETURN %'), {full_regex_utf8})
			
			UNION ALL

			-- Coinbase
			SELECT `hash`, `timestamp` AS `block_timestamp`, 0 as `value`, STRUCT(
					'coinbase' AS `type`,
					ARRAY_TO_STRING(REGEXP_EXTRACT_ALL(`coinbase_param`, REGEX_UTF8), '') AS `data`
			) AS `outputs`
			FROM `bigquery-public-data.crypto_bitcoin.blocks`
			WHERE 
			-- First 16 characters represent block height. Arbitrary data begins at position 17.
			REGEXP_CONTAINS(SUBSTR(`coinbase_param`, 17), {full_regex_utf8})

			{limit}
		""".format(
			regex_utf8=TextAnalysis.REGEX_UTF8,
			full_regex_utf8=f"r'^{TextAnalysis.REGEX_UTF8}*$'",
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
				print(tx)
				hex_value = tx['outputs']['data']
				if hex_value and not len(hex_value) % 2:
					insert(
						tx['hash'],
						codecs.decode(hex_value, 'hex'),
						int(tx['value']),
						tx['block_timestamp'].strftime('%Y-%m-%d %H:%M:%S'),
						tx['outputs']['type']
					)
					print('*', end='')
				else:
					print('\nNOT INSERTED', tx)
			print("Success!")
		except Exception as e:
			print("Something went really wrong!")
			print(e)
