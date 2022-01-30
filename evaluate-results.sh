#!/bin/bash

LC_NUMERIC=en_US

for chainD in ./files/*; do
    chain=$(basename ${chainD})
    echo ==== $(echo ${chain} | tr "[a-z]" "[A-Z]") ====
    echo

    # Files
    echo '[Files Results]'
    for typeD in ${chainD}/*; do
        for suffixD in ${typeD}/*; do
            echo $(basename ${typeD})/$(basename ${suffixD}): $(ls ${suffixD} | wc -l | xargs)
        done
    done
    echo

    # Text
    echo '[Text Results]'
    echo
    total=$(sqlite3 results.db "SELECT COUNT(*) FROM text_results WHERE chain = '${chain}'")
    printf "Total: %'.f\n" $total
    tokens=$(sqlite3 results.db "SELECT COUNT(*) FROM text_results WHERE chain = '${chain}' AND data LIKE '% %'")
    printf "Tokens: %'.f\n" $tokens
    texts=$((total-tokens))
    printf "Texts: %'.f\n" ${texts}
    if [ $chain == 'eth' ]; then
        to_contract=$(sqlite3 results.db "SELECT COUNT(*) FROM text_results WHERE chain = '${chain}' AND to_contract = 1")
        printf "Sent to contract: %'.f\n" $to_contract
    fi
    urls=$(python3 ./processing/count_urls.py ${chain})
    printf "Contain URL: %'.f\n" $urls
    emails=$(python3 ./processing/count_emails.py ${chain})
    printf "Contain Email Address: %'.f\n" $emails
    jsons=$(python3 ./processing/count_jsons.py ${chain})
    printf "Contain JSON: %'.f\n" $jsons
    hexs=$(sqlite3 results.db "SELECT COUNT(*) FROM text_results WHERE chain = '${chain}' AND data REGEXP '0x[A-Fa-f0-9]{2,}'")
    printf "Contain HEX: %'.f\n" $hexs
    pgps=$(sqlite3 results.db "SELECT COUNT(*) FROM text_results WHERE chain = '${chain}' AND data REGEXP '-----\{*\}*.+-----+\{*\}*.+-----\{*\}*.+-----'")
    printf "Contain PGP: %'.f\n" $pgps
    xmls=$(sqlite3 results.db "SELECT COUNT(*) FROM text_results WHERE chain = '${chain}' AND data LIKE '%</%'")
    printf "Contain XML: %'.f\n" $xmls
    data_uris=$(sqlite3 results.db "SELECT COUNT(*) FROM text_results WHERE chain = '${chain}' AND data LIKE 'data:%;base64,%'")
    printf "Contain Data URI: %'.f\n" $data_uris
    echo
    echo "Most Frequent Values:"
    for ((i=0; i<10; i++)); do
        row=$(sqlite3 results.db "SELECT COUNT(data) || '\t| ' || data FROM text_results WHERE chain = '${chain}' GROUP BY data ORDER BY COUNT(data) DESC LIMIT 1 OFFSET ${i}")
        rank=$((i+1))
        echo "${rank}.\t| ${row}"
    done
    echo
done
