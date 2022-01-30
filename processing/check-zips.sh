#!/bin/bash

broken=0
good=0
deleted_ids=()

printf "[ZIP ANALYSIS - START]"

for filename in $(find ./files/**/application/zip -type f); do
    unzip -Z "$filename" > /dev/null 2>&1
    if [ $? -ne 0 ]
    then
        printf "-"
        rm $filename
        deleted_ids+=("$(basename -- $filename .zip)")
        broken=$((broken+1))
    else
        file "$filename" | grep 'Microsoft Word' > /dev/null
        if [ $? -eq 0 ]; then
            mv "$filename" "$filename.doc"
        fi
        good=$((good+1))
    fi
done

if [ "${#deleted_ids[@]}" -gt 0 ]
then
    ids=$(printf ",\"%s\"" "${deleted_ids[@]}")
    sqlite3 results.db "DELETE FROM files_results WHERE content_type = 'application/zip' AND hash IN (${ids:1})"
fi

echo "[ZIP ANALYSIS - END]"

echo "Broken: $broken"
echo "Good: $good"
