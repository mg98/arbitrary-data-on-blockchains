#!/bin/bash

broken=0
good=0
deleted_ids=()

printf "[ZIP ANALYSIS - START]"

for filename in $(find ./results/**/application/zip -type f); do
    unzip -Z "$filename" > /dev/null 2>&1
    if [ $? -ne 0 ]
    then
        printf "-"
        rm $filename
        deleted_ids+=("$(basename -- $filename .zip)")
        broken=$((broken+1))
    else
        good=$((good+1))
    fi
done

if [ "${#deleted_ids[@]}" -gt 0 ]
then
    ids=$(printf ",\"%s\"" "${deleted_ids[@]}")
    tmpfile=$(mktemp)
    echo "DELETE FROM files_results WHERE mime_type = 'application/zip' AND hash IN (${ids:1})" > $tmpfile
    sqlite3 results.db < $tmpfile
    rm $tmpfile
fi

echo "[ZIP ANALYSIS - END]"

echo "Broken: $broken"
echo "Good: $good"
