import logging
from datetime import datetime
import json
import configparser
from typing import List

from elasticsearch import Elasticsearch, helpers
from indra.literature.pubmed_client import get_metadata_from_xml_tree

from ftp_client import NihFtpClient

logging.basicConfig(
    level=logging.INFO, format=f"[%(asctime)s] %(name)s %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


config = configparser.ConfigParser()
config.read("config.ini")

es = Elasticsearch(
    config.get("elasticsearch", "host"),
    ca_certs=config.get("elasticsearch", "ca_certs"),
    basic_auth=(
        config.get("elasticsearch", "username"),
        config.get("elasticsearch", "password"),
    ),
)
index_name = config.get("index", "name")
type_name = config.get("index", "type")


def create_pubmed_paper_index():
    with open("elastic-search-config.json", "r") as file_handle:
        es_config = json.load(file_handle)

    # We need to delete the index first before building a new
    if es.indices.exists(index=index_name):
        es.indices.delete(index=index_name)

    # Build the index.
    es.indices.create(
        index=index_name, ignore=400, body={"mappings": es_config["mappings"]}
    )
    return


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
        try:
            for is_ok, response in helpers.streaming_bulk(
                es, recent_articles, index=index_name, raise_on_error=True
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


def main():
    client = NihFtpClient("pubmed")
    baseline_files = client.list("baseline")
    baseline_gz_files = [fname for fname, _ in baseline_files if fname.endswith(".gz")]

    # We are currently just playing with the 100 files at the end of the list.
    file_paths = [f"baseline/{fname}" for fname in baseline_gz_files]
    return index_pubmed_files(file_paths[-100:])


if __name__ == "__main__":
    main()
