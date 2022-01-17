#!/bin/bash

broken=0
good=0
deleted_ids=()

function timeout() { perl -e 'alarm shift; exec @ARGV' "$@"; }

printf "[AUDIO ANALYSIS - START]"

for filename in $(find ./results/**/audio -type f \( -iname '*.mp3' -o -iname '*.aiff' -o -iname '*.flac' \)); do
	timeout 3 afplay -t 1 -v 0 "$filename" > /dev/null 2>&1
    if [ $? -ne 0 ]
    then
        printf "-"
        rm $filename
        deleted_ids+=("$(basename -- $filename .${filename##*.})")
        broken=$((broken+1))
    else
        good=$((good+1))
    fi
done

if [ "${#deleted_ids[@]}" -gt 0 ]
then
    ids=$(printf ",\"%s\"" "${deleted_ids[@]}")
    tmpfile=$(mktemp)
    echo "DELETE FROM files_results WHERE mime_type LIKE 'audio/%' AND hash IN (${ids:1})" > $tmpfile
    sqlite3 results.db < $tmpfile
    rm $tmpfile
fi

echo "[AUDIO ANALYSIS - END]"

echo "Broken: $broken"
echo "Good: $good"