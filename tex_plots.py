import sqlite3
import matplotlib.pyplot as plt
from dateutil import parser
import tikzplotlib

conn = sqlite3.connect("results.db")

def get_plot_data(query: str, date = True) -> tuple[list[str], list[any]]:
	cursor = conn.execute(query)
	data = cursor.fetchall()
	dates, values = [], []
	for row in data:
		dates.append(parser.parse(row[0]) if date else row[0])
		values.append(row[1])
	return dates, values

plt.plot_date(*get_plot_data("SELECT strftime('%Y-%m', block_timestamp) AS dt, COUNT(*) AS c FROM files_results WHERE method = 'Embedded' AND block_timestamp < '2021-10-17' AND deleted = 0 GROUP BY dt"), '-')

conn.close()

tikzplotlib.save("test.tex")
#plt.show()
