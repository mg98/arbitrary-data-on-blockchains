from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlparse
import sqlite3
import json
import re

HOST = "localhost"
PORT = 8080

head = """
<!DOCTYPE html>
<html>
<head>
	<title>Ethereum Data Analysis</title>
	<link rel="stylesheet" href="style.css">
	<style type="text/css">
		html, body {
			font-family: Arial, sans-serif;
		}
		table {
			border-collapse: collapse;
			margin-bottom: 1em;
		}
		th {
			text-align: left;
		}
		th, td {
			border: 1px solid black;
			padding: 4px;
			font-size:  12px;
		}
		td embed {
			max-width: 600px;
			max-height: 100px;
		}
	</style>
</head>
<body>
"""
tail = """
</body>
</html>
"""

conn = sqlite3.connect("results.db")

class WebServer(BaseHTTPRequestHandler):
	def do_GET(self):
		self.send_response(200)
		self.send_header("Content-type", "text/html")
		self.end_headers()
		self.write(head)

		params = urlparse(self.path)
		mode = params.path[1:]
		page = int(params.query[2:]) if params.query[:2] == 'p=' else 1

		if mode == "files":
			self.render_files_analysis(page)
		elif mode == "text":
			self.render_text_analysis(page)

		self.write(tail)

	def write(self, content: str):
		"""Convenience function to write to the HTML output."""
		self.wfile.write(bytes(content, "utf-8"))

	def render_files_analysis(self, page):
		nav = """
			<div class="nav">
				<a href="?p={}">Prev</a>
				<a href="?p={}">Next</a>
			</div>
			""".format(page-1, page+1)

		self.write(nav)
		self.write("<table><tr><th>Hash</th><th>Type</th><th>Method</th><th>Content</th></tr>")

		cursor = conn.execute("SELECT * FROM files_results LIMIT 20 OFFSET {}".format(20 * (page-1)))
		for row in cursor:
			hash_val, mime_type, method, data = row[0], row[1], row[2], row[3]

			if mime_type.startswith("application/") and mime_type != "application/pdf":
				data_displayed = f"""<a href="data:{mime_type};base64,{data}">Download</a>"""
			else:
				data_displayed = f"""<embed src="data:{mime_type};charset=utf-8;base64,{data}" type="{mime_type}">"""

			self.write(f"""
				<tr>
					<td><a href="https://etherscan.io/tx/{hash_val}" target="_blank">{hash_val}</a></td>
					<td>{mime_type}</td>
					<td>{method}</td>
					<td>{data_displayed}</td>
				</tr>
				""")

		self.write("</table>")
		self.write(nav)

	def render_text_analysis(self, page):
		# regex patterns
		REGEX_PATTERN_URL = '(https?:\/\/(?:www\.|(?!www))[a-zA-Z0-9][a-zA-Z0-9-]+[a-zA-Z0-9]\.[^\s]{2,}|www\.[a-zA-Z0-9][a-zA-Z0-9-]+[a-zA-Z0-9]\.[^\s]{2,}|https?:\/\/(?:www\.|(?!www))[a-zA-Z0-9]+\.[^\s]{2,}|www\.[a-zA-Z0-9]+\.[^\s]{2,})'
		REGEX_PATTERN_EMAIL = """(?:[a-z0-9!#$%&'*+/=?^_`{|}~-]+(?:\.[a-z0-9!#$%&'*+/=?^_`{|}~-]+)*|"(?:[\x01-\x08\x0b\x0c\x0e-\x1f\x21\x23-\x5b\x5d-\x7f]|\\[\x01-\x09\x0b\x0c\x0e-\x7f])*")@(?:(?:[a-z0-9](?:[a-z0-9-]*[a-z0-9])?\.)+[a-z0-9](?:[a-z0-9-]*[a-z0-9])?|\[(?:(?:(2(5[0-5]|[0-4][0-9])|1[0-9][0-9]|[1-9]?[0-9]))\.){3}(?:(2(5[0-5]|[0-4][0-9])|1[0-9][0-9]|[1-9]?[0-9])|[a-z0-9-]*[a-z0-9]:(?:[\x01-\x08\x0b\x0c\x0e-\x1f\x21-\x5a\x53-\x7f]|\\[\x01-\x09\x0b\x0c\x0e-\x7f])+)\])"""
		REGEX_PATTERN_HEX = '0x[A-Fa-f0-9]*($| )'
		REGEX_PATTERN_PGP = '-----{*}*.+-----+{*}*.+-----{*}*.+-----'
		REGEX_PATTERN_HTML = '<[^>]+>.*<\/[^>]+>'
		REGEX_PATTERN_JSON = '([{\[].*?[}\]])'

		def write_row(label: str, value):
			"""Write HTML for table row with two columns: label and value."""
			self.write("<tr><td>{}</td><td>{}</td></tr>".format(label, value))

		def count_rows_matching_regex(cursor: sqlite3.Cursor, pattern: str):
			return len(list(filter(lambda row: re.compile(pattern).match(row[0].decode("utf-8")), cursor)))

		self.write("<strong>General</strong>")
		self.write("<table><tr><th>Attribute</th><th>Amount</th></tr>")

		total = conn.execute("SELECT COUNT(*) FROM text_results").fetchone()[0]
		total_texts = conn.execute("SELECT COUNT(*) FROM text_results WHERE data LIKE '% %'").fetchone()[0]

		write_row("Total", total)
		write_row("Tokens", total-total_texts)
		write_row("Texts", total_texts)

		# count urls
		cursor = conn.execute("SELECT data FROM text_results WHERE data LIKE '%http%' OR data LIKE '%www.%'")
		write_row("Contain URL", count_rows_matching_regex(cursor, REGEX_PATTERN_URL))

		# count email addresses
		cursor = conn.execute("SELECT data FROM text_results WHERE data LIKE '%@%'")
		write_row("Contain Email Address", count_rows_matching_regex(cursor, REGEX_PATTERN_EMAIL))

		# count jsons
		cursor = conn.execute("SELECT data FROM text_results WHERE data LIKE '%{%}%'")
		total_jsons = 0
		for row in cursor:
			for candidate in re.compile(REGEX_PATTERN_JSON).findall(row[0].decode("utf-8")):
				if candidate == '{}': continue
				try:
					json.loads(row[0])
					total_jsons += 1
					break
				except:
					pass
		write_row("Contain JSON", total_jsons)

		# count hex
		cursor = conn.execute("SELECT data FROM text_results WHERE data LIKE '%0x%'")
		write_row("Contain HEX", count_rows_matching_regex(cursor, REGEX_PATTERN_HEX))

		# count pgp
		cursor = conn.execute("SELECT data FROM text_results WHERE data LIKE '%-----%'")
		write_row("Contain PGP", count_rows_matching_regex(cursor, REGEX_PATTERN_PGP))

		# count html
		cursor = conn.execute("SELECT data FROM text_results WHERE data LIKE '%</%'")
		write_row("Contain HTML/XML", count_rows_matching_regex(cursor, REGEX_PATTERN_HTML))

		self.write("</table>")

		self.write("<strong>Most Frequently</strong>")
		self.write("<table><tr><th>Text</th><th>Amount</th></tr>")
		cursor = conn.execute("SELECT data, COUNT(data) as count FROM text_results GROUP BY data HAVING count >= 1000 ORDER BY count DESC")
		for row in cursor: write_row(row[0].decode("utf-8"), row[1])

		self.write("</table>")


if __name__ == "__main__":
	s = HTTPServer((HOST, PORT), WebServer)
	print(f"""Server started! 🎉

View results for
- Files Analysis at http://{HOST}:{PORT}/files
- Text Analysis at http://{HOST}:{PORT}/text
""")

	try:
		s.serve_forever()
	except KeyboardInterrupt:
		pass

	s.server_close()
	conn.close()
	print("Server stopped.")
