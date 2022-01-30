#!/bin/bash

broken=0
good=0
deleted_ids=()

printf "[VIDEO ANALYSIS - START]"

for filename in $(find ./files/**/video -type f); do
    ffmpeg -v error -i  "$filename"  -f null - 2>&1 > /dev/null 2>&1
    if [ $? -ne 0 ]
    then
        printf "-"
        rm $filename
        deleted_ids+=("$(basename -- $filename .${filename##*.})")
        broken=$((broken+1))
    else
        good=$((good+1))
    fi

    if [ "${#deleted_ids[@]}" -gt 100 ]
    then
        ids=$(printf ",\"%s\"" "${deleted_ids[@]}")
        sqlite3 results.db "DELETE FROM files_results WHERE content_type LIKE 'video/%' AND hash IN (${ids:1})"
        deleted_ids=()
    fi
done

if [ "${#deleted_ids[@]}" -gt 0 ]
then
    ids=$(printf ",\"%s\"" "${deleted_ids[@]}")
    sqlite3 results.db "DELETE FROM files_results WHERE content_type LIKE 'video/%' AND hash IN (${ids:1})"
fi

echo "[VIDEO ANALYSIS - END]"

echo "Broken: $broken"
echo "Good: $good"
