import sqlite3
import base64
import os

conn = sqlite3.connect('results.db')

for row in conn.execute("SELECT chain, hash, mime_type, data FROM files_results WHERE chain='btc' and deleted=0"):
    chain, tx_hash, mime_type, data = str(row[0]), str(row[1]), str(row[2]), str(row[3])
    file_suffix = mime_type.split('/')[1]
    full_path = f'./results/{chain}/{mime_type}/{tx_hash}.{file_suffix}'
    dir_path = '/'.join(full_path.split('/')[:-1])
    if not os.path.exists(dir_path): os.makedirs(dir_path)
    with open(full_path, 'wb') as file_to_save:
        decoded_data = base64.decodebytes(data.encode('utf-8'))
        file_to_save.write(decoded_data)
