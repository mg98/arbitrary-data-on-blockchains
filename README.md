# Analyzer for Arbitrary Content on Bitcoin and Ethereum

[![DOI](https://img.shields.io/badge/DOI-10.1145%2F3487553.3524628-blue)](https://doi.org/10.1145/3487553.3524628)

## Background

This software was built as a toolkit for analyses of arbitrary contents (such as files and text messages) on blockchain-based systems and as the base for the results presented in the research article linked above.

## Requirements

- ✅ Python 3.9+ and PIP
- ✅ GCP account with enabled BigQuery API
- ✅ Post-processing of file results requires `afplay`, `djpeg`, `ffmpeg` and `unzip` _(each pre-installed on macOS)_

## Getting Started

First, make sure you have set the environment variable declaring the path to your Google Cloud credentials ([see how](https://cloud.google.com/docs/authentication/getting-started#setting_the_environment_variable)).

Note that the `analyze.py` script is going to create or update the file `results.db` in the project root. Also make sure to have your file permissions set up appropriately in this regard.

You will also have to install the python libraries required by this project. To do this, run the following command:

```
pip3 install -r requirements.txt
```

### 🕵️‍♂️ Run Analyzer

Running this script will scan the Bitcoin or Ethereum blockchain via BigQuery for arbitrary content and store the results in a SQLite database (`results.db`). The analyzer has three modes and will accordingly either scan for files of [popular type](./analysis/files/file-signatures.json) or embedded UTF-8 text strings. The third mode (`url`) exhaustively scans Ethereum transactions for HTTP and IPFS URLs. __Exhaustive__ means it will also find the URLs inside a smart contract deployment or call. This mode does not exist for Bitcoin, because those would already get caught by its text analysis.

```
usage: analyze.py [-h] [--limit LIMIT] [--content-types CONTENT_TYPES] {btc,eth} {files,text,url}

Analyze blockchains for arbitrary content.

positional arguments:
  {btc,eth}             Blockchain to analyze.
  {files,text,url}      Type of content to look for (URL analysis exists only for ETH).

optional arguments:
  -h, --help            show this help message and exit
  --limit LIMIT         Limit the results processed by the BigQuery SQL query. If not set, proceeds to query the entire blockchain.
  --content-types CONTENT_TYPES
                        Comma separated list of content types to be considered for files analysis (default: '*').
```

Run `sh full-analysis.sh` to perform a **complete** analysis with a single command.

### ⚙️ Post-Process File Results

The file analyses will store the findings as base64-encoded strings in the database, including also many false positives (files we are unable to view using standard software). Use the following script to
- export all results to your disk
- verify files using macOS utilities and eliminate most false positives
```
sh post-process.sh
```

Please check the remaining files manually and delete unviewable files yourself.

### 📖 View Results

> :warning: **Warning:** Results may include explicit content or even content that is considered illegal in your country.

Having followed _"Post-Process File Results"_, you will find all files in the subfolders of `./files`.
Please use an SQLite browser to view individual text results.

For general insights and statistics you can execute the following script:
```
sh evaluate-results.sh
```

The following script will use the data to plot some graphs, e.g. to display the frequency of the transactions over time. This can be done by running the following command:
```
python3 plots.py
```
