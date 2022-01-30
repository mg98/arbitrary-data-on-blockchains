#!/bin/bash

[ -e ./files ] && rm -r ./files
python3 export_results.py

sh processing/check-zips.sh
sh processing/check-jpegs.sh
sh processing/check-audios.sh
sh processing/check-videos.sh
