import argparse
import text_analysis
import files_analysis

parser = argparse.ArgumentParser(description='Analyze the Ethereum blockchain for arbitrary content.')
parser.add_argument('mode', choices=['files', 'text'], help='Type of content to look for.')
parser.add_argument('--limit', help='Limit the results processed by the BigQuery SQL query. If not set, proceeds to query the entire blockchain.')
args = parser.parse_args()

if args.mode == 'files': files_analysis.run(args.limit)
elif args.mode == 'text': text_analysis.run(args.limit)
