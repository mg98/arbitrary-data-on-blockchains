from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import parse_qs
import os, sys, sqlite3

host_name = "localhost"
server_port = 8080

head = """
<!DOCTYPE html>
<html>
<head>
    <title>Ethereum Data Found</title>
    <link rel="stylesheet" href="style.css">
    <style type="text/css">
        html, body {
            font-family: Arial, sans-serif;
        }
        table {
            border-collapse: collapse;
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
    </table>
</body>
</html>
"""

conn = sqlite3.connect("results.db")

class MyServer(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-type", "text/html")
        self.end_headers()
        self.wfile.write(bytes(head, "utf-8"))

        params = parse_qs(self.path[2:])
        page = int(params["p"][0]) if "p" in params else 1

        nav = """
        <div class="nav">
            <a href="?p={}">Prev</a>
            <a href="?p={}">Next</a>
        </div>
        """.format(page-1, page+1)

        self.wfile.write(bytes(nav, "utf-8"))
        self.wfile.write(bytes("<table><tr><th>Hash</th><th>Type</th><th>Method</th><th>Content</th></tr>", "utf-8"))

        cursor = conn.execute("SELECT * FROM results LIMIT 20 OFFSET {}".format(20 * (page-1)))
        for row in cursor:
            self.wfile.write(bytes("""
                <tr>
                    <td><a href="https://etherscan.io/tx/{hash}" target="_blank">{hash}</td>
                    <td>{mime_type}</td>
                    <td>{method}</td>
                    <td><embed src="data:{mime_type};charset=utf-8;base64,{data}" type="{mime_type}"></td>
                </tr>
                """.format(
                    hash=row[0],
                    mime_type=row[1],
                    method=row[2],
                    data=row[3]
                ), "utf-8"))

        self.wfile.write(bytes("</table>", "utf-8"))
        self.wfile.write(bytes(nav, "utf-8"))
        self.wfile.write(bytes(tail, "utf-8"))

if __name__ == "__main__":        
    webServer = HTTPServer((host_name, server_port), MyServer)
    print("Server started http://%s:%s" % (host_name, server_port))

    try:
        webServer.serve_forever()
    except KeyboardInterrupt:
        pass

    webServer.server_close()
    conn.close()
    print("Server stopped.")