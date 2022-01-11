from .files import FilesAnalysis
from google.cloud import bigquery

class EthFilesAnalysis(FilesAnalysis):
	"""Files Analysis for the Ethereum blockchain."""

	"""Identifier of analyzed blockchain."""
	CHAIN = 'eth'

	def run_core(self):
		"""Runs the query on BigQuery and persists results to the database."""

		sigs = [v for values in self.file_signatures.values() for v in values]
		sigs_for_injected = [v for mime_type in self.file_signatures.keys() for v in self.file_signatures[mime_type] if not self.is_expensive_type(mime_type)]
		sql_likes = list(map(lambda fs: "`input` LIKE '0x{}%'".format(fs), sigs)) + list(map(lambda fs: "`input` LIKE '0x%{}%'".format(fs), sigs_for_injected))

		# reset table
		if self.reset: self.conn.execute("DELETE FROM files_results WHERE chain = ?", (EthFilesAnalysis.CHAIN,))

		# build where clause
		sql_where = ' OR '.join(sql_likes)
		if not self.reset:
			last_record = self.conn.execute("SELECT block_timestamp FROM files_results ORDER BY block_timestamp DESC LIMIT 1").fetchone()
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
			self.conn.execute("""
                INSERT INTO files_results (
                    chain, hash, mime_type, method, block_timestamp, to_contract, data
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (EthFilesAnalysis.CHAIN, hash, mime_type, method, block_timestamp, to_contract, data))

		try:
			for tx in query_job:
				print(tx["hash"])
				mime_type = self.get_mime_type(tx['input'])

				for sig in self.file_signatures[mime_type]:
					start = tx["input"].find(sig)
					# consider only embedded or not expensive injected files
					if start != -1 and (start == 2 or not self.is_expensive_type(mime_type)):
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
			print("Success!")
		except Exception as e:
			print("Something went really wrong!")
			print(e)
