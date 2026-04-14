"""
PubMed Fetcher — fetch, batch, and summarise PubMed abstracts.

Credentials: set ENTREZ_EMAIL and ENTREZ_API_KEY environment variables.
"""

from __future__ import annotations

import argparse
import logging
import os
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path

import tiktoken
from Bio import Entrez

# ---------------------------------------------------------------------------
# Credentials — fail loudly if not set; no hardcoded fallbacks
# ---------------------------------------------------------------------------
def _require_env(var: str) -> str:
    value = os.getenv(var)
    if not value:
        raise EnvironmentError(
            f"Required environment variable '{var}' is not set.\n"
            f"Export it before running:\n  export {var}=your_value"
        )
    return value

Entrez.email   = _require_env("ENTREZ_EMAIL")
Entrez.api_key = _require_env("ENTREZ_API_KEY")

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
FETCH_BATCH_SIZE  = 50
NCBI_RATE_DELAY   = 0.34   # seconds — keeps requests ≤ 3/s
MAX_RETRIES       = 3
RETRY_BACKOFF     = 2.0    # seconds; doubles each attempt
NCBI_SERVER_MAX   = 9_999  # NCBI hard cap for esearch retmax

# ---------------------------------------------------------------------------
# Data models
# ---------------------------------------------------------------------------
@dataclass
class Article:
    pmid: str
    title: str
    abstract: str
    word_count: int = 0
    token_count: int = 0


@dataclass
class BatchStats:
    batch_number: int
    article_count: int
    token_count: int


@dataclass
class FetchStats:
    total_articles: int = 0
    skipped_articles: int = 0
    total_words: int = 0
    total_tokens: int = 0
    batches: list[BatchStats] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Retry decorator
# ---------------------------------------------------------------------------
def _with_retries(fn, *args, retries: int = MAX_RETRIES, backoff: float = RETRY_BACKOFF, **kwargs):
    """Call fn(*args, **kwargs), retrying on transient exceptions."""
    last_exc: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            return fn(*args, **kwargs)
        except Exception as exc:
            last_exc = exc
            wait = backoff * (2 ** (attempt - 1))
            logger.warning(
                "Attempt %d/%d failed: %s — retrying in %.1fs",
                attempt, retries, exc, wait,
            )
            time.sleep(wait)
    raise RuntimeError(f"All {retries} attempts failed") from last_exc


# ---------------------------------------------------------------------------
# Core class
# ---------------------------------------------------------------------------
class PubMedFetcher:
    """Search PubMed and write token-batched abstracts to disk."""

    def __init__(
        self,
        query: str,
        years: int = 5,
        max_results: int = 100,
        token_limit: int = 8_000,
        output_dir: str | Path = "./pubmed_output",
        review_only: bool = False,
    ) -> None:
        if not query.strip():
            raise ValueError("query must not be empty")
        if years < 1:
            raise ValueError("years must be ≥ 1")
        if max_results < 1:
            raise ValueError("max_results must be ≥ 1")
        if token_limit < 50:
            raise ValueError("token_limit is too small to hold even one article")

        self.query        = query
        self.years        = years
        # Clamp silently and warn — don't silently drop data
        if max_results > NCBI_SERVER_MAX:
            logger.warning(
                "max_results=%d exceeds NCBI server cap of %d; clamping.",
                max_results, NCBI_SERVER_MAX,
            )
        self.max_results  = min(max_results, NCBI_SERVER_MAX)
        self.token_limit  = token_limit
        self.output_dir   = Path(output_dir)
        self.review_only  = review_only

        self._tokenizer = tiktoken.get_encoding("cl100k_base")
        self._stats     = FetchStats()

        self.output_dir.mkdir(parents=True, exist_ok=True)
        self._configure_file_logging()

        logger.info("PubMed Fetcher initialised")
        logger.info("Query       : %s", self.query)
        logger.info("Date range  : last %d year(s)", self.years)
        logger.info("Max results : %d", self.max_results)
        logger.info("Review only : %s", self.review_only)
        logger.info("Token limit : %d per batch", self.token_limit)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def run(self) -> None:
        """Full pipeline: search → stream-fetch → batch → summarise."""
        pmids = self._search()
        if not pmids:
            logger.warning("No PMIDs returned. Exiting.")
            return

        # Stream articles through batching — avoid loading all into RAM
        self._stream_batch_and_save(pmids)
        self._write_summary()
        self._log_summary()

    # ------------------------------------------------------------------
    # Search
    # ------------------------------------------------------------------

    def _build_query(self) -> str:
        return (
            f'{self.query} AND "review"[Publication Type]'
            if self.review_only
            else self.query
        )

    def _date_range(self) -> tuple[str, str]:
        end   = datetime.now()
        start = end - timedelta(days=365 * self.years)
        return start.strftime("%Y/%m/%d"), end.strftime("%Y/%m/%d")

    def _search(self) -> list[str]:
        query           = self._build_query()
        mindate, maxdate = self._date_range()
        logger.info("Searching PubMed: %s", query)

        def _do_search():
            with Entrez.esearch(
                db="pubmed",
                term=query,
                mindate=mindate,
                maxdate=maxdate,
                datetype="pdat",
                retmax=self.max_results,
                usehistory="y",
            ) as handle:
                return Entrez.read(handle)

        try:
            record = _with_retries(_do_search)
        except RuntimeError:
            logger.exception("Search permanently failed after retries")
            return []

        pmids       = record["IdList"]
        total_count = int(record["Count"])

        if total_count > self.max_results:
            logger.warning(
                "PubMed has %d total results but only %d were requested. "
                "Increase --max-results to fetch more.",
                total_count, self.max_results,
            )

        logger.info("Total on server: %d  |  Retrieving: %d", total_count, len(pmids))
        return pmids

    # ------------------------------------------------------------------
    # Fetch + stream batching (memory-efficient)
    # ------------------------------------------------------------------

    def _stream_batch_and_save(self, pmids: list[str]) -> None:
        """
        Fetch articles one NCBI-batch at a time and immediately feed them
        into the token batcher — never holds all articles in RAM at once.
        """
        current_batch:   list[Article] = []
        current_tokens:  int           = 0
        batch_number:    int           = 1
        total_processed: int           = 0

        for batch_start in range(0, len(pmids), FETCH_BATCH_SIZE):
            chunk    = pmids[batch_start : batch_start + FETCH_BATCH_SIZE]
            articles = self._fetch_batch_with_retry(chunk)

            for article in articles:
                combined = f"{article.title} {article.abstract}"
                article.word_count  = len(combined.split())

                # Count tokens including the file overhead written per article
                overhead = f"PMID: {article.pmid}\nTITLE: \nABSTRACT:\n{'-'*40}\n"
                article.token_count = self._count_tokens(combined + overhead)

                self._stats.total_words  += article.word_count
                self._stats.total_tokens += article.token_count

                # Handle articles larger than the entire token_limit
                if article.token_count > self.token_limit:
                    logger.warning(
                        "PMID %s alone uses %d tokens (limit=%d); "
                        "writing as its own oversized batch.",
                        article.pmid, article.token_count, self.token_limit,
                    )
                    if current_batch:
                        self._write_batch(current_batch, batch_number, current_tokens)
                        batch_number  += 1
                        current_batch  = []
                        current_tokens = 0
                    self._write_batch([article], batch_number, article.token_count)
                    batch_number += 1
                    continue

                if current_tokens + article.token_count > self.token_limit and current_batch:
                    self._write_batch(current_batch, batch_number, current_tokens)
                    current_batch, current_tokens = [], 0
                    batch_number += 1

                current_batch.append(article)
                current_tokens += article.token_count

            total_processed += len(chunk)
            logger.info("Fetched %d / %d PMIDs", total_processed, len(pmids))
            time.sleep(NCBI_RATE_DELAY)

        if current_batch:
            self._write_batch(current_batch, batch_number, current_tokens)

        self._stats.total_articles = total_processed - self._stats.skipped_articles

    def _fetch_batch_with_retry(self, pmids: list[str]) -> list[Article]:
        def _do_fetch():
            with Entrez.efetch(
                db="pubmed",
                id=",".join(pmids),
                rettype="xml",
                retmode="xml",
            ) as handle:
                return Entrez.read(handle)

        try:
            records = _with_retries(_do_fetch)
        except RuntimeError:
            logger.error(
                "Permanently failed to fetch batch starting at PMID %s — skipping %d articles",
                pmids[0], len(pmids),
            )
            self._stats.skipped_articles += len(pmids)
            return []

        articles = []
        for record in records.get("PubmedArticle", []):
            article = self._parse_record(record)
            if article is not None:
                articles.append(article)
        return articles

    def _parse_record(self, record: dict) -> Article | None:
        try:
            citation = record["MedlineCitation"]
            pmid     = str(citation["PMID"])
            title    = str(citation["Article"]["ArticleTitle"])

            abstract_data = citation["Article"].get("Abstract")
            if not abstract_data or "AbstractText" not in abstract_data:
                logger.debug("PMID %s has no abstract — skipping", pmid)
                self._stats.skipped_articles += 1
                return None

            parts    = abstract_data["AbstractText"]
            abstract = (
                " ".join(str(p) for p in parts)
                if isinstance(parts, list)
                else str(parts)
            )

            # Sanitise: strip null bytes that corrupt text files
            title    = title.replace("\x00", "")
            abstract = abstract.replace("\x00", "")

            return Article(pmid=pmid, title=title, abstract=abstract)

        except KeyError:
            logger.warning("Incomplete record — skipping", exc_info=True)
            self._stats.skipped_articles += 1
            return None

    # ------------------------------------------------------------------
    # Disk I/O
    # ------------------------------------------------------------------

    def _count_tokens(self, text: str) -> int:
        return len(self._tokenizer.encode(text))

    def _write_batch(
        self, batch: list[Article], batch_number: int, token_count: int
    ) -> None:
        path = self.output_dir / f"pubmed_batch_{batch_number}.txt"

        # Guard against concurrent overwrites in multi-process runs
        if path.exists():
            logger.warning("Batch file %s already exists — appending suffix", path.name)
            path = self.output_dir / f"pubmed_batch_{batch_number}_{int(time.time())}.txt"

        lines: list[str] = []
        for article in batch:
            lines.extend([
                f"PMID: {article.pmid}", "",
                f"TITLE: {article.title}", "",
                "ABSTRACT:", article.abstract, "",
                "-" * 40, "",
            ])

        try:
            path.write_text("\n".join(lines), encoding="utf-8")
        except OSError as exc:
            logger.error("Failed to write %s: %s — batch data lost!", path, exc)
            raise  # Surface disk-full / permission errors immediately

        logger.info(
            "Batch %d → %d articles, %d tokens → %s",
            batch_number, len(batch), token_count, path.name,
        )
        self._stats.batches.append(
            BatchStats(batch_number=batch_number, article_count=len(batch), token_count=token_count)
        )

    # ------------------------------------------------------------------
    # Reporting
    # ------------------------------------------------------------------

    def _configure_file_logging(self) -> None:
        log_path     = self.output_dir / "fetch_log.txt"
        file_handler = logging.FileHandler(log_path, encoding="utf-8")
        file_handler.setFormatter(
            logging.Formatter("%(asctime)s  %(levelname)-8s  %(message)s")
        )
        logger.addHandler(file_handler)

    def _write_summary(self) -> None:
        stats     = self._stats
        avg_words  = stats.total_words  // stats.total_articles if stats.total_articles else 0
        avg_tokens = stats.total_tokens // stats.total_articles if stats.total_articles else 0

        lines = [
            "=" * 60, "PUBMED FETCH SUMMARY", "=" * 60, "",
            f"Search Query    : {self.query}",
            f"Review Only     : {self.review_only}",
            f"Date Range      : last {self.years} year(s)",
            f"Fetch Date      : {datetime.now():%Y-%m-%d %H:%M:%S}", "",
            "-" * 60, "FETCH STATISTICS", "-" * 60,
            f"Articles fetched  : {stats.total_articles}",
            f"Articles skipped  : {stats.skipped_articles}",
            f"Batches created   : {len(stats.batches)}", "",
            "-" * 60, "TOKEN & WORD METRICS", "-" * 60,
            f"Total words        : {stats.total_words:,}",
            f"Total tokens       : {stats.total_tokens:,}",
            f"Avg words/article  : {avg_words}",
            f"Avg tokens/article : {avg_tokens}", "",
            "-" * 60, "BATCH DISTRIBUTION", "-" * 60,
            *(
                f"Batch {b.batch_number:>3}: {b.article_count} articles, {b.token_count:,} tokens"
                for b in stats.batches
            ),
            "", "-" * 60, "OUTPUT FILES", "-" * 60,
            f"Output directory: {self.output_dir}",
            *(f"  - pubmed_batch_{b.batch_number}.txt" for b in stats.batches),
            "  - summary.txt", "  - fetch_log.txt",
        ]

        summary_path = self.output_dir / "summary.txt"
        summary_path.write_text("\n".join(lines), encoding="utf-8")
        logger.info("Summary written to %s", summary_path)

    def _log_summary(self) -> None:
        s = self._stats
        logger.info("=" * 60)
        logger.info("FINAL SUMMARY")
        logger.info("Query          : %s", self.query)
        logger.info("Total articles : %d", s.total_articles)
        logger.info("Skipped        : %d", s.skipped_articles)
        logger.info("Total batches  : %d", len(s.batches))
        logger.info("Total words    : %s", f"{s.total_words:,}")
        logger.info("Total tokens   : %s", f"{s.total_tokens:,}")
        if s.total_articles:
            logger.info("Avg tokens/art : %d", s.total_tokens // s.total_articles)
        logger.info("Output dir     : %s", self.output_dir)
        logger.info("=" * 60)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Fetch PubMed article abstracts and split them into "
            "token-limited batch files ready for LLM ingestion."
        ),
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        epilog=(
            "Credentials must be supplied via environment variables:\n"
            "  export ENTREZ_EMAIL=you@example.com\n"
            "  export ENTREZ_API_KEY=your_ncbi_key\n\n"
            "Example:\n"
            '  python pubmed_fetcher.py \\\n'
            '    --query \'"Computational Biology/history"[MAJR]\' \\\n'
            "    --years 40 --max-results 2500 --review-only"
        ),
    )
    parser.add_argument("--query",        required=True,                    help="PubMed search query string")
    parser.add_argument("--years",        type=int,   default=5,            help="How many years back to search")
    parser.add_argument("--max-results",  type=int,   default=100,          help="Maximum number of articles to retrieve", dest="max_results")
    parser.add_argument("--token-limit",  type=int,   default=8_000,        help="Token ceiling per output batch file",    dest="token_limit")
    parser.add_argument("--output-dir",               default="./pubmed_output", help="Directory for all output files",   dest="output_dir")
    parser.add_argument("--review-only",  action="store_true",              help="Restrict results to review articles",    dest="review_only")
    return parser


def main() -> None:
    args = _build_parser().parse_args()
    PubMedFetcher(
        query=args.query,
        years=args.years,
        max_results=args.max_results,
        token_limit=args.token_limit,
        output_dir=args.output_dir,
        review_only=args.review_only,
    ).run()


if __name__ == "__main__":
    main()
