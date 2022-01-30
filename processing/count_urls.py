import sqlite3, re, sys

REGEX_PATTERN_URL = '(https?:\/\/(?:www\.|(?!www))[a-zA-Z0-9][a-zA-Z0-9-]+[a-zA-Z0-9]\.[^\s]{2,}|www\.[a-zA-Z0-9][a-zA-Z0-9-]+[a-zA-Z0-9]\.[^\s]{2,}|https?:\/\/(?:www\.|(?!www))[a-zA-Z0-9]+\.[^\s]{2,}|www\.[a-zA-Z0-9]+\.[^\s]{2,})'
conn = sqlite3.connect('results.db')
cursor = conn.execute("SELECT data FROM text_results WHERE chain = ? AND data LIKE '%http%' OR data LIKE '%www.%'", (sys.argv[1],))
total_urls = len(list(filter(lambda row: re.compile(REGEX_PATTERN_URL).match(row[0].decode("utf-8")), cursor)))
conn.close()
print(total_urls)