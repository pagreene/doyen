import pickle
from collections import Counter, defaultdict
from indra.literature.pubmed_client import get_metadata_from_xml_tree
from ftp_client import NihFtpClient
from database.models import AuthorInfo, InstitutionInfo
from database.schema import *

from pydantic.error_wrappers import ValidationError

def upload():
    with open("completed_files.pkl", "rb") as f:
        completed_files = pickle.load(f)

    client = NihFtpClient("pubmed")
    for baseline_file, _ in client.list("baseline")[::-1]:
        if not baseline_file.endswith(".gz"):
            continue

        if baseline_file in completed_files:
            print(f"Skipping {baseline_file}.")
            continue

        authors = set()
        mesh_terms = defaultdict(lambda: defaultdict(lambda: 0))
        papers = defaultdict(list)

        print(baseline_file)
        xml_file = client.get_xml_file(f"baseline/{baseline_file}")

        articles = [article for article in get_metadata_from_xml_tree(xml_file).values()]

        for article in articles:
            for author_info in article["authors"]:
                if not any(v for v in author_info.values()):
                    continue
                if not author_info["last_name"]:
                    print(f"No last name for author: {author_info}")
                    continue
                try:
                    author = AuthorInfo(**author_info)
                except ValidationError as err:
                    print(f"Could not parts author info: {err}")
                    continue
                authors.add(author)
                for mesh_entry in article["mesh_annotations"]:
                    mesh_key = (mesh_entry["mesh"], mesh_entry["text"])
                    mesh_terms[author.get_durable_hash()][mesh_key] += 1
                papers[author.get_durable_hash()].append(article['pmid'])
        batch_update(authors, mesh_terms, papers)

        completed_files.append(baseline_file)

        with open("completed_files.pkl", "wb") as f:
            pickle.dump(completed_files, f)


if __name__ == "__main__":
    upload()