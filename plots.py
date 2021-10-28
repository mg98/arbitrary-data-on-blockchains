import sqlite3
import matplotlib.pyplot as plt
from dateutil import parser
import numpy as np

def get_plot_data(query: str, date = True) -> tuple[list[str], list[any]]:
	cursor = conn.execute(query)
	data = cursor.fetchall()
	dates, values = [], []
	for row in data:
		dates.append(parser.parse(row[0]) if date else row[0])
		values.append(row[1])
	return dates, values

fig, axs = plt.subplots(2, 2)
fig.set_size_inches(12, 8)

conn = sqlite3.connect("results.db")

axs[0, 0].set_title("Frequency of Text Transactions")
axs[0, 0].plot_date(*get_plot_data("SELECT strftime('%Y-%m', block_timestamp) AS dt, COUNT(*) AS c FROM text_results GROUP BY dt"), '-')
axs[0, 0].set(xlabel="Time",ylabel="Transaction amount per month")

axs[0, 1].set_title("Frequency of Text Length")
axs[0, 1].plot(*get_plot_data("""
SELECT LENGTH(data) AS data_length, COUNT(*) AS c
FROM text_results
GROUP BY data_length
""", False), '-')
axs[0, 1].set(xlabel="Text length in characters", ylabel="Transaction amount")
axs[0, 1].set_xscale('log')

axs[1, 0].set_title("Frequency of Files Transactions")
axs[1, 0].plot_date(*get_plot_data("SELECT strftime('%Y-%m', block_timestamp) AS dt, COUNT(*) AS c FROM files_results WHERE deleted = 0 GROUP BY dt"), '-')
axs[1, 0].set(xlabel="Time",ylabel="Transaction amount per month")

axs[1, 1].set_title("Frequency of Files Transactions (Embedded Only)")
axs[1, 1].plot_date(*get_plot_data("SELECT strftime('%Y-%m', block_timestamp) AS dt, COUNT(*) AS c FROM files_results WHERE method = 'Embedded' AND deleted = 0 GROUP BY dt"), '-')
axs[1, 1].set(xlabel="Time",ylabel="Transaction amount per month")

conn.close()
plt.tight_layout(pad=1)
plt.show()
