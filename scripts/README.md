# Scripts

These scripts are used in [../post-process.sh](../post-process.sh) to further process files that have been found on the blockchains.

The scripts aim at eradicating false positives, i.e., files that are not readable.
To evaluate this, the script makes use of `djpeg`, `afplay`, `ffmpeg`, and `unzip` - which are all standard utilities under macOS!

Broken files get removed from disk and the `deleted` flag in the database gets set on the respective record.

These scripts do not replace manual file review, but they do significantly narrow down the result set for potential matches.

All scripts must be executed from the project root directory because of relative paths!

Also note that the scripts are not suitable for parallel execution.