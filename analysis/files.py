from google.cloud import bigquery
import codecs
import sqlite3
import json
from fnmatch import fnmatch

class FilesAnalysis:
	"""Analysis of transaction input data that contain popular file types."""

	def __init__(self, limit: int = 0, reset: bool = False, mime_types: list[str] = ['*']):
		"""
		Initialize files analysis.

		:param limit Limit results processed by BigQuery.
		:param reset Flag about resetting the database before starting the analysis.
		:param mime_types List of considerable mime types for this analysis. Asterix-sign supported.
		"""
		self.limit = limit
		self.reset = reset
		self.file_signatures = FilesAnalysis.get_file_signatures(mime_types)

	def __enter__(self):
		self.conn = sqlite3.connect("results.db")
		return self

	def __exit__(self, type, val, tb):
		self.conn.close()

	@staticmethod
	def get_file_signatures(mimes: list[str]) -> dict[str,list[str]]:
		"""Returns dict of file signatures filtered by `mimes`."""
		with open('file-signatures.json') as f: file_signatures = json.load(f)
		return {
			mime_type : file_signatures[mime_type]
			for mime_type in list(
				filter(lambda k: any(fnmatch(k, mime) for mime in mimes), file_signatures)
			)
		}

	def get_mime_type(self, input):
		"""Returns mime type detected in input (candidate with most signature digits)."""
		top_candidate = (None, 0) # tuple of mime type and signature length
		for (mime_type, sigs) in self.file_signatures.items():
			for sig in sigs:
				if sig in input:
					if top_candidate[1] < len(sig):
						top_candidate = (mime_type, len(sig))
		return top_candidate[0]

	@staticmethod
	def hex_to_base64(hex_value: str):
		"""Converts hex to base64."""
		return codecs.encode(codecs.decode(hex_value, 'hex'), 'base64').decode()

	@staticmethod
	def is_expensive_type(mime_type: str) -> bool:
		"""Check if mime type is considered expensive for injected content analysis (too many false positives)."""
		return not mime_type.startswith("image") or mime_type == "image/jpeg"

	def run(self):
		"""Runs the query on BigQuery and persists results to the database."""

		sigs = [v for values in self.file_signatures.values() for v in values]
		sigs_for_injected = [v for mime_type in self.file_signatures.keys() for v in self.file_signatures[mime_type] if not FilesAnalysis.is_expensive_type(mime_type)]
		sql_likes = list(map(lambda fs: "`input` LIKE '0x{}%'".format(fs), sigs)) + list(map(lambda fs: "`input` LIKE '0x%{}'".format(fs), sigs_for_injected))

		# create or reset table
		cursor = self.conn.cursor()
		if self.reset: cursor.execute("DROP TABLE IF EXISTS files_results2")
		cursor.execute("CREATE TABLE IF NOT EXISTS files_results2 (hash TEXT, mime_type TEXT, method TEXT, to_contract BOOLEAN, data TEXT, block_timestamp DATETIME, deleted BOOLEAN DEFAULT 0)")
		self.conn.commit()

		# build where clause
		sql_where = ' OR '.join(sql_likes)
		if not self.reset:
			last_record = self.conn.execute("SELECT block_timestamp FROM files_results2 ORDER BY block_timestamp DESC LIMIT 1").fetchone()
			if last_record: sql_where = f't.`block_timestamp` > \'{last_record[0]}\' AND ({sql_where})'

		query = """
			SELECT `hash`, `input`, t.`block_timestamp`,
			CASE WHEN c.`address` IS NOT NULL THEN true ELSE false END AS to_contract
			FROM `bigquery-public-data.crypto_ethereum.transactions` t
			LEFT OUTER JOIN `bigquery-public-data.crypto_ethereum.contracts` c
			ON c.`address` = `to_address`
			WHERE {} {}
		""".format(sql_where, 'LIMIT {}'.format(self.limit) if self.limit is not None else '')

		client = bigquery.Client()
		query_job = client.query(query)

		print("Writing results to db...")

		def insert(hash: str, mime_type: str, method: str, block_timestamp: str, to_contract: bool, data: str):
			cursor.execute("""INSERT INTO files_results2 (
				hash, mime_type, method, block_timestamp, to_contract, data
			) VALUES (?, ?, ?, ?, ?, ?)""", (hash, mime_type, method, block_timestamp, to_contract, data))
			self.conn.commit()

		try:
			for tx in query_job:
				print(tx["hash"])
				mime_type = self.get_mime_type(tx['input'])

				for sig in self.file_signatures[mime_type]:
					start = tx["input"].find(sig)
					# consider only embedded or not expensive injected files
					if start != -1 and (start == 2 or not FilesAnalysis.is_expensive_type(mime_type)):
						hex_value = tx["input"][start:]
						break

				if hex_value and not len(hex_value) % 2:
					insert(
						tx['hash'],
						mime_type,
						"Embedded" if start == 2 else "Injected",
						tx['block_timestamp'].strftime('%Y-%m-%d %H:%M:%S'),
						tx['to_contract'],
						FilesAnalysis.hex_to_base64(hex_value)
					)
				else:
					print("Failed to decode input.")
		except Exception as e:
			print("Something went really wrong!")
			print(e)
