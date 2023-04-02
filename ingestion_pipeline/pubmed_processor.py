import logging
from datetime import datetime
import json
import configparser
from typing import List
from pathlib import Path
import shutil

import click
from elasticsearch import Elasticsearch, helpers, BadRequestError
from indra.literature.pubmed_client import get_metadata_from_xml_tree

from .ftp_client import NihFtpClient

# Define file paths relative to this location.
HERE = Path(__file__).parent.absolute()
CONFIG_FILE = HERE / "resources" / "config.ini"
CONFIG_TEMPLATE = HERE / "resources" / "config_template.ini"
ES_INDEX_CONFIG = HERE / "resources" / "elastic-search-config.json"

# Create a logger with nice time stamps.
logging.basicConfig(
    level=logging.INFO, format=f"[%(asctime)s] %(name)s %(levelname)s - %(message)s"
)
logger = logging.getLogger("doyen_pubmed_upload")


# Make sure the user fills out the config file.
if not CONFIG_FILE.exists():
    logger.warning(f"No config file found. It should be at {CONFIG_FILE}.")
    shutil.copy(CONFIG_TEMPLATE, CONFIG_FILE)
    logger.warning(
        "Created a new config file from a template. Please "
        "update the config with current values."
    )
    input("After you update the values, press any key to continue..")


# Load the config.
CONFIG = configparser.ConfigParser()
CONFIG.read(CONFIG_FILE)


def get_es_client():
    """Get an instance of the elasticsearch client."""
    return Elasticsearch(
        CONFIG.get("elasticsearch", "host"),
        ca_certs=CONFIG.get("elasticsearch", "ca_certs"),
        basic_auth=(
            CONFIG.get("elasticsearch", "username"),
            CONFIG.get("elasticsearch", "password"),
        ),
    )


def create_pubmed_paper_index():
    """Create the pubmed paper index in elasticsearch."""
    es = get_es_client()

    with ES_INDEX_CONFIG.open() as file_handle:
        es_config = json.load(file_handle)

    # We need to delete the index first before building a new
    index_name = CONFIG.get("index", "name")
    if es.indices.exists(index=index_name):
        logger.info("Deleting the pre-existing index.")
        es.indices.delete(index=index_name)

    # Build the index.
    try:
        es.indices.create(index=index_name, body={"mappings": es_config["mappings"]})
    except BadRequestError as err:
        logger.error("Failed to create the index.")
        logger.exception(err)
        return False

    return True


def index_pubmed_files(file_paths: List[str], refresh_index: bool = True):
    """Indexes all the gz files in the provided list_of_files parameter"""
    if refresh_index:
        logger.info("Rebuilding the index...")
        create_pubmed_paper_index()

    client = NihFtpClient("pubmed")

    # Loop over all files, extract the information and index in bulk
    files_indexed = 0
    articles_added = 0
    for i, file_path in enumerate(file_paths):
        logger.info(f"Processing file {i} of {len(file_paths)}: {file_path}")
        start = datetime.now()

        # Parse the XML
        xml_tree = client.get_xml_tree(file_path)
        articles_by_pmid = get_metadata_from_xml_tree(
            xml_tree, get_abstracts=True, mesh_annotations=True, detailed_authors=True
        )
        recent_articles = [
            article
            for article in articles_by_pmid.values()
            if article["publication_date"]["year"] > 2017
        ]
        xml_tree.clear()

        # Update the date format.
        for article in recent_articles:
            pub_date = article["publication_date"]
            article["publication_date"] = datetime(
                year=pub_date["year"], month=pub_date["month"], day=pub_date["day"]
            ).strftime("%Y-%m-%d")

        # Make sure we didn't filter out all the articles.
        if not recent_articles:
            logger.info(f"No recent articles found in {file_path}, continuing...")
            continue

        # Try to upload all these articles into ElasticSearch
        logger.info(f"Starting to index {len(recent_articles)} articles...")
        start_index = datetime.now()
        es = get_es_client()
        try:
            for is_ok, response in helpers.streaming_bulk(
                es,
                recent_articles,
                index=CONFIG.get("index", "name"),
                raise_on_error=True,
            ):
                if not is_ok:
                    # If the indexing is not successful, log the error
                    identifier = response["create"]["_id"]
                    error = response["create"]["error"]
                    logger.error(f"Error processing document ID {identifier}")
                    logger.error(json.dumps(error, indent=4, ensure_ascii=False))
        except Exception as err:
            logger.error(f"Failed to add documents from {file_path}: {err}")
            logger.exception(err)
            continue

        total_time_taken = datetime.now() - start
        indexing_time_take = datetime.now() - start_index
        logger.info(
            f"Completed file {i} of {len(file_paths)}, {file_path}, "
            f"in {total_time_taken} total, and {indexing_time_take}"
        )

        files_indexed += 1
        articles_added += len(recent_articles)

    logger.info(
        f"Successfully index {files_indexed} files, adding {articles_added} articles."
    )


@click.command()
@click.option(
    "--start",
    "-s",
    default=None,
    help="The index in the list of baselines from which to start. The value "
    "can be either positive, or negative to measure from the end of the list.",
    type=int,
)
@click.option(
    "--end",
    "-e",
    default=None,
    help="The index in the list of baseline files at which to end. The value "
    "can be either positive, or negative to measure from teh end of the list.",
    type=int,
)
@click.option(
    "--quiet",
    "-q",
    is_flag=True,
    help="Optionally quiet the logs to only show warnings.",
)
def fill_elasticsearch(start: int | None, end: int | None, quiet: bool):
    # Set the log level.
    if quiet:
        logging.basicConfig(level=logging.WARNING)

    # Get the list of baseline files
    client = NihFtpClient("pubmed")
    baseline_files = client.list("baseline")
    baseline_gz_files = sorted(
        fname for fname, _ in baseline_files if fname.endswith(".gz")
    )

    # We are currently just playing with the 100 files at the end of the list.
    file_paths = [f"baseline/{fname}" for fname in baseline_gz_files]
    return index_pubmed_files(file_paths[start:end])


if __name__ == "__main__":
    fill_elasticsearch()
