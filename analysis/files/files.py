import codecs
import sqlite3
import json
from fnmatch import fnmatch
from abc import ABC, abstractmethod

class FilesAnalysis(ABC):
	"""Abstraction for analysis of transaction input data that contain popular file types."""

	def __init__(self, chain: str, limit: int = 0, content_types: list[str] = ['*']):
		"""
		Initialize files analysis.

		:param chain Blockchain.
		:param limit Limit results processed by BigQuery.
		:param content_types List of considerable content types for this analysis. Asterix-sign supported.
		"""
		self.chain = chain
		self.limit = limit
		self.file_signatures = FilesAnalysis.get_file_signatures(content_types)

	def __enter__(self):
		self.conn = sqlite3.connect("results.db")
		return self

	def __exit__(self, type, val, tb):
		self.conn.close()

	def insert(self, hash: str, content_type: str, method: str, block_timestamp: str, type: str, data: str, to_contract: bool = False):
		self.conn.execute("""
			INSERT INTO files_results (
				chain, hash, content_type, method, block_timestamp, type, data, to_contract
			) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		""", (self.chain, hash, content_type, method, block_timestamp, type, data, to_contract))
		self.conn.commit()

	@staticmethod
	def get_file_signatures(content_types: list[str]) -> dict[str,list[str]]:
		"""Returns dict of file signatures filtered by `content_types`."""
		with open('analysis/files/file-signatures.json') as f: file_signatures = json.load(f)
		return {
			content_type : file_signatures[content_type]
			for content_type in list(
				filter(lambda k: any(fnmatch(k, ct) for ct in content_types), file_signatures)
			)
		}

	def get_content_type(self, input):
		"""Returns content type detected in input (candidate with most signature digits)."""
		top_candidate = (None, 0) # tuple of content type and signature length
		for (content_type, sigs) in self.file_signatures.items():
			for sig in sigs:
				if sig in input:
					if top_candidate[1] < len(sig):
						top_candidate = (content_type, len(sig))
		return top_candidate[0]

	@staticmethod
	def hex_to_base64(hex_value: str):
		"""Converts hex to base64."""
		return codecs.encode(codecs.decode(hex_value, 'hex'), 'base64').decode()

	def run(self):
		"""Runs the query on BigQuery and persists results to the database."""

		# setup database
		self.conn.execute("""
			CREATE TABLE IF NOT EXISTS files_results (
				chain TEXT, 
				hash TEXT, 
				content_type TEXT, 
				method TEXT, 
				to_contract BOOLEAN, 
				type TEXT, 
				data TEXT, 
				block_timestamp DATETIME, 
				deleted BOOLEAN DEFAULT 0
			)
		""")
		self.conn.execute("DELETE FROM files_results WHERE chain = ?", (self.chain,))
		self.conn.commit()

		self.run_core()

	@abstractmethod
	def run_core(self):
		"""Runs the query on BigQuery and persists results to the database."""
		raise NotImplementedError("Must override run_core")
