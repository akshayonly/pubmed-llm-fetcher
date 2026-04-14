# PubMed Fetcher

A command-line tool that searches PubMed, fetches article abstracts, and splits them into token-limited batch files ready for LLM ingestion pipelines.

---

## Table of Contents

- [Features](#features)
- [Requirements](#requirements)
- [Installation](#installation)
- [Credentials Setup](#credentials-setup)
- [Usage](#usage)
  - [CLI](#cli)
  - [Python API](#python-api)
- [Options](#options)
- [Output Structure](#output-structure)
- [Examples](#examples)
- [Security Notes](#security-notes)
- [Troubleshooting](#troubleshooting)

---

## Features

- Full PubMed search with date range filtering and optional review-article restriction
- Token-accurate batching (includes file overhead in count) using `cl100k_base` tokenizer
- Streaming pipeline — RAM usage stays proportional to fetch batch size, not total results
- Automatic retry with exponential backoff on transient NCBI network failures
- Oversized article detection — single articles exceeding the token limit are isolated rather than dropped
- Null-byte sanitisation to prevent corrupt output files
- Concurrent-write collision detection with timestamp-suffixed fallback filenames
- Structured summary and log files written alongside batch output

---

## Requirements

- Python 3.10+
- An [NCBI account](https://www.ncbi.nlm.nih.gov/account/) with an API key

```
biopython
tiktoken
```

---

## Installation

```bash
git clone https://github.com/your-org/pubmed-fetcher.git
cd pubmed-fetcher
pip install biopython tiktoken
```

---

## Credentials Setup

**Never hardcode credentials.** The tool requires two environment variables and will fail loudly at startup if either is missing.

```bash
export ENTREZ_EMAIL=you@example.com
export ENTREZ_API_KEY=your_ncbi_api_key
```

To make these permanent, add the lines above to your `~/.bashrc`, `~/.zshrc`, or equivalent shell profile.

To get an NCBI API key:
1. Sign in at https://www.ncbi.nlm.nih.gov/account/
2. Go to **Account Settings → API Key Management**
3. Click **Create an API Key**

> Without an API key, NCBI throttles requests to 3/second. With a key the limit rises to 10/second. The tool's built-in rate delay (`0.34s`) respects the unauthenticated limit; with a key you can safely lower it if needed.

---

## Usage

### CLI

```bash
python pubmed_fetcher.py \
  --query '"Computational Biology/history"[MAJR]' \
  --years 40 \
  --max-results 2500 \
  --token-limit 10000 \
  --output-dir ./compbio_hist \
  --review-only
```

Run `python pubmed_fetcher.py --help` for the full option reference.

### Python API

```python
from pubmed_fetcher import PubMedFetcher

fetcher = PubMedFetcher(
    query='"Computational Biology/history"[MAJR]',
    years=40,
    max_results=2500,
    token_limit=10_000,
    output_dir="./compbio_hist",
    review_only=True,
)
fetcher.run()
```

---

## Options

| Flag | Type | Default | Description |
|---|---|---|---|
| `--query` | `str` | *(required)* | PubMed search query. Supports full MeSH syntax. |
| `--years` | `int` | `5` | How many years back from today to search. |
| `--max-results` | `int` | `100` | Maximum articles to retrieve. Capped at NCBI's server limit of 9,999. |
| `--token-limit` | `int` | `8000` | Token ceiling per batch file. |
| `--output-dir` | `str` | `./pubmed_output` | Directory where all output files are written. Created if it doesn't exist. |
| `--review-only` | flag | `False` | Restrict results to review articles only. |

---

## Output Structure

```
output_dir/
├── pubmed_batch_1.txt   # Articles 1–N (≤ token_limit tokens)
├── pubmed_batch_2.txt   # Articles N+1–M
├── ...
├── summary.txt          # Fetch statistics and batch distribution
└── fetch_log.txt        # Full timestamped run log
```

### Batch file format

Each entry within a batch file follows this structure:

```
PMID: 12345678

TITLE: Example Article Title

ABSTRACT:
This is the abstract text...

----------------------------------------

```

### summary.txt

Includes:

- Search query and parameters used
- Total articles fetched and skipped
- Total words and tokens across all articles
- Per-batch breakdown (article count, token count)
- Full list of output files

---

## Examples

**Fetch recent RNA-seq method reviews:**
```bash
python pubmed_fetcher.py \
  --query '"RNA-Seq/methods"[MAJR] AND "Computational Biology/methods"[MeSH]' \
  --years 3 \
  --max-results 200 \
  --review-only \
  --output-dir ./rnaseq_reviews
```

**Fetch all genomics papers from a specific institution:**
```bash
python pubmed_fetcher.py \
  --query '"Genomics"[MeSH] AND "Agharkar Research Institute"[AD]' \
  --years 10 \
  --max-results 500 \
  --output-dir ./ari_genomics
```

**Large historical fetch with bigger batches:**
```bash
python pubmed_fetcher.py \
  --query '"Human Genome Project"[MAJR]' \
  --years 35 \
  --max-results 9999 \
  --token-limit 50000 \
  --output-dir ./hgp_history
```

---

## Security Notes

- Credentials are read exclusively from environment variables. The tool raises `EnvironmentError` at startup if either is unset — there are no hardcoded fallbacks.
- Do not commit `.env` files or shell profiles containing your API key to version control. Add them to `.gitignore`.
- If you suspect your API key has been exposed, rotate it immediately at https://www.ncbi.nlm.nih.gov/account/.

---

## Troubleshooting

**`EnvironmentError: Required environment variable 'ENTREZ_EMAIL' is not set`**
Export the variable in your shell before running — see [Credentials Setup](#credentials-setup).

**`WARNING: PubMed has X total results but only Y were requested`**
Increase `--max-results`. Note the hard ceiling is 9,999 per NCBI's API.

**Batch files are smaller than expected**
Token counts include file overhead (PMID label, TITLE label, ABSTRACT label, separator line) in addition to the article text itself. This is intentional to ensure batches stay within the limit when loaded by downstream tools.

**`WARNING: PMID XXXXXXXX alone uses N tokens (limit=M); writing as its own oversized batch`**
A single article's abstract is longer than `--token-limit`. Either increase the limit or note that this batch will exceed it when consumed downstream.

**Network errors / partial fetches**
The tool retries each NCBI request up to 3 times with exponential backoff. If all retries fail, the affected batch of up to 50 articles is skipped and logged. Check `fetch_log.txt` for details.

## License

This project is licensed under CC BY-NC 4.0 - see the [LICENSE](LICENSE) file for details.
