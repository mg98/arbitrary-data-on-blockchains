import argparse
import analysis

parser = argparse.ArgumentParser(description='Analyze the Ethereum blockchain for arbitrary content.')

parser.add_argument('mode', choices=['files', 'text'], help='Type of content to look for.')
parser.add_argument('--limit', help='Limit the results processed by the BigQuery SQL query. If not set, proceeds to query the entire blockchain.')
parser.add_argument(
	'--reset',
	help='If set, current results get cleared out beforehand and a full analysis is done.',
	default=False,
	action=argparse.BooleanOptionalAction
)
parser.add_argument('--mimes', help='Comma separated list of mime types to be considered for files analysis (default: \'*\').', default='*')
parser.add_argument(
	'--skip-injected-jpegs',
	help="Search for injected jpegs in files analysis produces many results with many false positives (approx. > 98%%)",
	default=True,
	action=argparse.BooleanOptionalAction
)

args = parser.parse_args()

if args.mode != 'files' and args.mimes != '*':
	parser.error('--mimes can only be set for file analysis.')


if args.mode == 'files':
	analyzer = analysis.BtcFilesAnalysis(
		limit=args.limit,
		reset=args.reset,
		mime_types=args.mimes.split(','),
		skip_injected_jpegs=args.skip_injected_jpegs
	)
elif args.mode == 'text':
	analyzer = analysis.BtcTextAnalysis(
		limit=args.limit, 
		reset=args.reset
	)

with analyzer as a: a.run()
