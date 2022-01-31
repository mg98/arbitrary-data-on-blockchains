#!/bin/bash

[ -e ./results.db ] && rm ./results.db
echo '========[ BTC Text ]========'
python3 analyze.py btc text
echo '========[ ETH Text ]========'
python3 analyze.py eth text
echo '========[ BTC Files ]========'
python3 analyze.py btc files
echo '========[ ETH Files ]========'
python3 analyze.py eth files
echo '========[ ETH URL ]========'
python3 analyze.py eth url
echo '========[ DONE ]========'