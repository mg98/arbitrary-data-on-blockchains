#!/bin/bash

broken=0
corrupted=0
good=0
unknown=0
deleted_ids=()

printf "[JPEG ANALYSIS - START]"

for filename in $(find ./files/**/image/jpeg -type f -name *.jpeg); do
    djpeg -fast -grayscale -onepass "$filename" > /dev/null 2>&1
    case $? in
        0)  good=$((good+1)) ;;
        1)  printf "-"
            rm $filename
            deleted_ids+=("$(basename -- $filename .jpeg)")
            broken=$((broken+1)) ;;
        2)  corrupted=$((corrupted+1)) ;;
        *)  unknown=$((unknown+1)) ;;
    esac
done

if [ "${#deleted_ids[@]}" -gt 0 ]
then
    ids=$(printf ",\"%s\"" "${deleted_ids[@]}")
    sqlite3 results.db "DELETE FROM files_results WHERE content_type = 'image/jpeg' AND hash IN (${ids:1})"
fi

echo "[JPEG ANALYSIS - END]"

echo "Broken: $broken"
echo "Corrupted: $corrupted"
echo "Good: $good"
[ $unknown -gt 0 ] && echo "Unknown: $unknown"
