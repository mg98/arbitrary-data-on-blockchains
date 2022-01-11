import codecs
import sqlite3
import json
from fnmatch import fnmatch
from abc import ABC, abstractmethod

class FilesAnalysis(ABC):
	"""Abstraction for analysis of transaction input data that contain popular file types."""

	def __init__(self, limit: int = 0, reset: bool = False, mime_types: list[str] = ['*'], skip_injected_jpegs: bool = True):
		"""
		Initialize files analysis.

		:param limit Limit results processed by BigQuery.
		:param reset Flag about resetting the database before starting the analysis.
		:param mime_types List of considerable mime types for this analysis. Asterix-sign supported.
		:param skip_injected_jpegs Do not search for injected jpegs.
		"""
		self.limit = limit
		self.reset = reset
		self.skip_injected_jpegs = skip_injected_jpegs
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

	def is_expensive_type(self, mime_type: str) -> bool:
		"""Check if mime type is considered expensive for injected content analysis (too many false positives)."""
		return not mime_type.startswith("image") or (mime_type == "image/jpeg" and self.skip_injected_jpegs)

	def run(self):
		"""Runs the query on BigQuery and persists results to the database."""

		# setup database
		self.conn.execute("""
			CREATE TABLE IF NOT EXISTS files_results (
				chain TEXT, 
				hash TEXT, 
				mime_type TEXT, 
				method TEXT, 
				to_contract BOOLEAN, 
				type TEXT, 
				data TEXT, 
				block_timestamp DATETIME, 
				deleted BOOLEAN DEFAULT 0
			)
		""")

		# reset
		if self.reset: self.conn.execute("DELETE FROM files_results")

		self.run_core()

	@abstractmethod
	def run_core(self):
		"""Runs the query on BigQuery and persists results to the database."""
		raise NotImplementedError("Must override run_core")
