import codecs
import binascii
import re
import sqlite3
from google.cloud import bigquery

class EthURLAnalysis:
	"""URL Analysis for the Ethereum blockchain."""

	def __init__(self, limit: int = 0):
		"""
		Initialize URL analysis.

		:param limit Limit results processed by BigQuery.
		"""
		self.limit = limit

	def __enter__(self):
		self.conn = sqlite3.connect("results.db")
		return self

	def __exit__(self, type, val, tb):
		self.conn.close()

	def setup_db(self):
		self.conn.execute("""
			CREATE TABLE IF NOT EXISTS eth_url_results (
				hash TEXT,
				type TEXT,
				url TEXT, 
				value UNSIGNED BIG INT, 
				block_timestamp DATETIME, 
				to_contract BOOLEAN
			)
		""")
		self.conn.execute("DELETE FROM eth_url_results")
		self.conn.commit()
	
	def insert(self, hash: str, type: str, url: str, value: int, block_timestamp: str, to_contract: bool = False):
		self.conn.execute("""
			INSERT INTO eth_url_results (
				hash, type, url, value, block_timestamp, to_contract
			) VALUES (?, ?, ?, ?, ?, ?)
		""", (hash, type, url, value, block_timestamp, to_contract))
		self.conn.commit()

	def run(self):
		"""Runs the query on BigQuery and persists results to the database."""

		self.setup_db()

		keywords = ['http://', 'https://', '.onion', 'ipfs://']
		keywords_hex = list(map(lambda kw: binascii.hexlify(bytearray(kw, 'utf8')).decode('utf8'), keywords))

		alphanum = list('ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789')
		alphanum_hex = list(map(lambda an: binascii.hexlify(bytearray(an, 'utf8')).decode('utf8'), alphanum))
		regex_alphanum_chars = '(' + '|'.join(alphanum_hex) + ')'

		# special chars possible in a (http) url
		special_chars = ['-', '.', '_', '~', ':', '/', '?', '#', '[', ']', '@', '!', '$', '&', '\'', '(', ')', '*', '+', ',', ';', '%', '=']
		special_chars_hex = list(map(lambda c: binascii.hexlify(bytearray(c, 'utf8')).decode('utf8'), special_chars))

		url_chars_pattern = '(' + '|'.join(alphanum_hex + special_chars_hex) + ')'	# possible chars in a url
		http_pattern = f'{keywords_hex[0]}{url_chars_pattern}{{5,}}' 				# http:// followed by at least 5 valid characters
		https_pattern = f'{keywords_hex[1]}{url_chars_pattern}{{5,}}' 				# https:// followed by at least 5 valid characters
		onion_pattern = f'{regex_alphanum_chars}{{16}}{keywords_hex[2]}' 			# 16 alphanum characters followed by .onion
		ipfs_pattern = f'{keywords_hex[3]}{url_chars_pattern}{{7,}}'				# ipfs: followed by at least 7 valid characters

		query = f"""
			DECLARE HTTP_PATTERN DEFAULT "{http_pattern}";
			DECLARE HTTPS_PATTERN DEFAULT "{https_pattern}";
			DECLARE ONION_PATTERN DEFAULT "{onion_pattern}";
			DECLARE IPFS_PATTERN DEFAULT "{ipfs_pattern}";
			DECLARE URL_CHARS_PATTERN DEFAULT "{url_chars_pattern}";

			SELECT `hash`, `input` AS `data`, ROUND(IEEE_DIVIDE(`value`, 1000000000)) AS value, t.`block_timestamp`,
			CASE WHEN c.`address` IS NOT NULL THEN true ELSE false END AS to_contract,
			'http' AS `type`
			FROM `bigquery-public-data.crypto_ethereum.transactions` t
			LEFT OUTER JOIN `bigquery-public-data.crypto_ethereum.contracts` c ON c.`address` = `to_address`
			WHERE REGEXP_CONTAINS(`input`, HTTP_PATTERN) OR REGEXP_CONTAINS(`input`, HTTPS_PATTERN) AND NOT REGEXP_CONTAINS(`input`, ONION_PATTERN)

			UNION ALL
			
			SELECT `hash`, `input` AS `data`, ROUND(IEEE_DIVIDE(`value`, 1000000000)) AS value, t.`block_timestamp`,
			CASE WHEN c.`address` IS NOT NULL THEN true ELSE false END AS to_contract,
			'ipfs' AS `type`
			FROM `bigquery-public-data.crypto_ethereum.transactions` t
			LEFT OUTER JOIN `bigquery-public-data.crypto_ethereum.contracts` c ON c.`address` = `to_address`
			WHERE REGEXP_CONTAINS(`input`, IPFS_PATTERN)

			UNION ALL

			SELECT `hash`, `input` AS `data`, ROUND(IEEE_DIVIDE(`value`, 1000000000)) AS value, t.`block_timestamp`,
			CASE WHEN c.`address` IS NOT NULL THEN true ELSE false END AS to_contract,
			'onion' AS `type`
			FROM `bigquery-public-data.crypto_ethereum.transactions` t
			LEFT OUTER JOIN `bigquery-public-data.crypto_ethereum.contracts` c ON c.`address` = `to_address`
			WHERE REGEXP_CONTAINS(`input`, ONION_PATTERN)

			{'LIMIT {}'.format(self.limit) if self.limit is not None else ''}
		"""

		client = bigquery.Client()
		query_job = client.query(query)

		print("Writing results to db...")

		try:
			for tx in query_job:
				hex_value = str(tx['data'])[2:]
				hex_value = [hex_value[i:i+2] for i in range(0, len(hex_value), 2)]
				hex_value = list(filter(lambda x: x != '00', hex_value))
				hex_value = ''.join(hex_value)

				data: bytes = codecs.decode(hex_value, 'hex')
				data = data.decode('ascii', errors='ignore')

				try:
					if tx['type'] == 'http':
						url = re.search('((https?:\/\/(?:www\.|(?!www)))|www\.)[a-zA-Z0-9][a-zA-Z0-9-]+[a-zA-Z0-9]\.[A-Za-z0-9\.]{2,}.[^{}:\"<>]*', data).group(0)
						if '/ipfs/' in url: tx['type'] = 'ipfs'
						if re.match('((https?:\/\/(?:www\.|(?!www)))|www\.)[a-zA-Z0-9][a-zA-Z0-9-]+[a-zA-Z0-9]\.onion(\/[^\s]*)?', data): tx['type'] = 'onion'
					elif tx['type'] == 'ipfs':
						url = re.search('ipfs:\/\/[a-zA-Z0-9]{16,}', data).group(0)
					elif tx['type'] == 'onion':
						url = re.search(f'([A-Za-z0-9]{{16}}|[A-Za-z0-9]{{56}})\.onion(\/[^\s]*)?', data).group(0)
				except Exception as e:
					continue
				
				self.insert(
					tx['hash'],
					tx['type'],
					url,
					int(tx['value']),
					tx['block_timestamp'].strftime('%Y-%m-%d %H:%M:%S'),
					tx['to_contract']
				)
				print('*', end='')
			print("Success!")
		except Exception as e:
			print("Something went really wrong!")
			print(e)
