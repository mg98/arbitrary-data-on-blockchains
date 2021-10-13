# An Analysis of Arbitrary Content on the Ethereum Blockchain

This project originated as a study project at *Humboldt University of Berlin*. The goal was to analyze the amount of classification of arbitrary content (such as images or audio) that has been persisted on the Ethereum blockchain.

## Requirements

- ✅ Python 3 and PIP
- ✅ GCP account with enabled BigQuery API

## Getting Started

First, make sure you have set the environment variable declaring the path to your Google Cloud credentials ([see how](https://cloud.google.com/docs/authentication/getting-started#setting_the_environment_variable)).

Note that the `analyze.py` script is going to create or update the file `results.db` in the project root. Also make sure to have your file permissions set up appropriately in this regard.

You will also have to install the python libraries required by this project. To do this, run the following command:

```
pip3 install -r requirements.txt
```

### 🕵️‍♂️ Run Analyzer on the Blockchain

Running this script will scan the Ethereum blockchain via BigQuery for arbitrary content and store the results in the SQLite database (`results.db`). The analyzer has two modes and will either scan for files of popular type or embedded UTF-8 text strings.

```
usage: analyze.py [-h] [--limit LIMIT] [--mimes MIMES] {files,text}

Analyze the Ethereum blockchain for arbitrary content.

positional arguments:
  {files,text}   Type of content to look for.

optional arguments:
  -h, --help     show this help message and exit
  --limit LIMIT  Limit the results processed by the BigQuery SQL query. If not set, proceeds to query the entire blockchain.
  --mimes MIMES  Comma separated list of mime types to be considered (default: '*').
```

### 📖 View Results

This project also includes a script to fire up a web server that reads the results from the database and expose them in HTML that you can access at http://localhost:8080/files and http://localhost:8080/text, respectively.

> :warning: **Warning:** Results may include very explicit content or even content that is considered illegal in your country.

Use the following command to run the server:

```
python3 server.py
```
