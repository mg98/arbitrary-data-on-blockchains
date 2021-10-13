import argparse
from analysis.files import FilesAnalysis
from analysis.text import TextAnalysis

parser = argparse.ArgumentParser(description='Analyze the Ethereum blockchain for arbitrary content.')

parser.add_argument('mode', choices=['files', 'text'], help='Type of content to look for.')
parser.add_argument('--limit', help='Limit the results processed by the BigQuery SQL query. If not set, proceeds to query the entire blockchain.')
parser.add_argument('--mimes', help='Comma separated list of mime types to be considered (default: \'*\').', default='*')
args = parser.parse_args()

if args.mode != 'files' and args.mimes:
    parser.error('--mimes can only be set for file analysis.')


if args.mode == 'files': FilesAnalysis(limit=args.limit, mime_types=args.mimes.split(',')).run()
elif args.mode == 'text': TextAnalysis(limit=args.limit).run()
