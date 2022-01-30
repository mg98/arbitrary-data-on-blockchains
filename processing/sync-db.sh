# I use this script after deleting invalid files from disk to help me syncing up with the database. Use with caution!

# JPEG
ids=()
for filename in $(find ./files/**/image/jpeg -type f -name *.jpeg); do
    ids+=("$(basename -- $filename .jpeg)")
done
ids_str=$(printf ",\"%s\"" "${ids[@]}")
sqlite3 results.db "DELETE FROM files_results WHERE content_type = 'image/jpeg' AND hash NOT IN (${ids_str:1})"

# PNG
ids=()
for filename in $(find ./files/**/image/png -type f -name *.png); do
    ids+=("$(basename -- $filename .png)")
done
ids_str=$(printf ",\"%s\"" "${ids[@]}")
sqlite3 results.db "DELETE FROM files_results WHERE content_type = 'image/png' AND hash NOT IN (${ids_str:1})"

# WEBP
ids=()
for filename in $(find ./files/**/image/webp -type f); do
    ids+=("$(basename -- $filename .webp)")
done
ids_str=$(printf ",\"%s\"" "${ids[@]}")
sqlite3 results.db "DELETE FROM files_results WHERE content_type = 'image/webp' AND hash NOT IN (${ids_str:1})"

# GIF
ids=()
for filename in $(find ./files/**/image/gif -type f); do
    ids+=("$(basename -- $filename .gif)")
done
ids_str=$(printf ",\"%s\"" "${ids[@]}")
sqlite3 results.db "DELETE FROM files_results WHERE content_type = 'image/gif' AND hash NOT IN (${ids_str:1})"

# MP3
ids=()
for filename in $(find ./files/**/audio/mp3 -type f); do
    ids+=("$(basename -- $filename .mp3)")
done
ids_str=$(printf ",\"%s\"" "${ids[@]}")
sqlite3 results.db "DELETE FROM files_results WHERE content_type = 'audio/mp3' AND hash NOT IN (${ids_str:1})"

# PDF
ids=()
for filename in $(find ./files/**/application/pdf -type f); do
    ids+=("$(basename -- $filename .pdf)")
done
ids_str=$(printf ",\"%s\"" "${ids[@]}")
sqlite3 results.db "DELETE FROM files_results WHERE content_type = 'application/pdf' AND hash NOT IN (${ids_str:1})"

# 7Z
ids=()
for filename in $(find ./files/**/application/7z -type f); do
    ids+=("$(basename -- $filename .7z)")
done
ids_str=$(printf ",\"%s\"" "${ids[@]}")
sqlite3 results.db "DELETE FROM files_results WHERE content_type = 'application/7z' AND hash NOT IN (${ids_str:1})"

# ZIP
ids=()
for filename in $(find ./files/**/application/zip -type f); do
    ids+=("$(basename -- $filename .zip)")
done
ids_str=$(printf ",\"%s\"" "${ids[@]}")
sqlite3 results.db "DELETE FROM files_results WHERE content_type = 'application/zip' AND hash NOT IN (${ids_str:1}, '0xebbadd18a9396e7cf7b78e2d66b51597253a0a8b4d3510f20a3f69e5877964c2')"
