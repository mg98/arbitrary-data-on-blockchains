import sqlite3, re, json, sys

conn = sqlite3.connect('results.db')
cursor = conn.execute('SELECT data FROM text_results WHERE chain = ? AND data LIKE ?', (sys.argv[1], '%{%}%'))
total_jsons = 0
for row in cursor:
    for candidate in re.compile('([{\[].*?[}\]])').findall(row[0].decode("utf-8")):
        if candidate == '{}': continue
        try:
            json.loads(candidate)
            total_jsons += 1
            break
        except:
            pass
conn.close()
print(total_jsons)