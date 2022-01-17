#!/bin/bash

[ -e ./results ] && rm -r ./results
python3 scripts/export_results.py

sh scripts/check-zips.sh
sh scripts/check-jpegs.sh
sh scripts/check-audios.sh
sh scripts/check-videos.sh
