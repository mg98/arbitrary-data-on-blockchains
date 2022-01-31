import argparse
import analysis

parser = argparse.ArgumentParser(description='Analyze blockchains for arbitrary content.')

parser.add_argument('chain', choices=['btc', 'eth'], help='Blockchain to analyze.')
parser.add_argument('mode', choices=['files', 'text', 'url'], help='Type of content to look for (URL analysis exists only for ETH).')
parser.add_argument('--limit', help='Limit the results processed by the BigQuery SQL query. If not set, proceeds to query the entire blockchain.')
parser.add_argument('--content-types', help='Comma separated list of content types to be considered for files analysis (default: \'*\').', default='*')

args = parser.parse_args()

if args.mode != 'files' and args.content_types != '*':
	parser.error('--content-types can only be set for file analysis.')


if args.mode == 'files':
	analyzer = (analysis.BtcFilesAnalysis if args.chain == 'btc' else analysis.EthFilesAnalysis)(
		limit=args.limit,
		content_types=args.content_types.split(',')
	)
elif args.mode == 'text':
	analyzer = (analysis.BtcTextAnalysis if args.chain == 'btc' else analysis.EthTextAnalysis)(
		limit=args.limit
	)
elif args.mode == 'url':
	analyzer = analysis.EthURLAnalysis(
		limit=args.limit
	)

with analyzer as a: a.run()
