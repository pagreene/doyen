import os, sys
import gzip
import datetime, time
import xml.etree.cElementTree as ET  # C implementation of ElementTree
import traceback
import json
import configparser

from elasticsearch import Elasticsearch, helpers


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
    settings = {
        # changing the number of shards after the fact is not possible max Gb per
        # shard should be 30Gb, replicas can be produced anytime
        # https://qbox.io/blog/optimizing-elasticsearch-how-many-shards-per-index
        "number_of_shards": 1,
        "number_of_replicas": 0,
    }
    mappings = {
        # "pubmed-paper": {
        "mappings": {
            "properties": {
                "pm_id": {"type": "keyword"},
                "title": {"type": "text", "analyzer": "standard"},
                "abstract": {"type": "string", "analyzer": "standard"},
                "created_date": {"type": "date", "format": "yyyy-MM-dd"},
                "authors": {
                    "properties": {
                        "last_name": {"type": "text"},
                        "first_name": {"type": "text"},
                        "ORCID": {"type": "text"},
                        "affiliations": {
                            "properties": {"affiliation": {"type": "text"}}
                        },
                    }
                },
                "mesh_annotations": {
                    "properties": {
                        "descriptor_mesh_id": {"type": "text"},
                        "descriptor_name": {"type": "text"},
                        "descriptor_major_topic": {"type": "boolean"},
                        "qualifier_names": {
                            "type": "nested",
                            "properties": {
                                "qualifier_mesh_id": {"type": "text"},
                                "qualifier_name": {"type": "text"},
                                "qualifier_major_topic": {"type": "boolean"},
                            },
                        },
                    }
                },
                "references": {"properties": {"article_id": {"type": "text"}}},
            }
        }
    }
    if es.indices.exists(index=index_name):
        # deleting index before creating new
        es.indices.delete(index=index_name)
    es.indices.create(index=index_name, ignore=400, body=mappings)

    return


def get_all_gz_files(pubmed_folder):
    """Gets the list of all the gz files available in the provided pubmed_folder"""
    return [
        os.path.join(pubmed_folder, f)
        for f in os.listdir(pubmed_folder)
        if os.path.isfile(os.path.join(pubmed_folder, f)) and f[-2:] == "gz"
    ]


def get_elasticsearch_doc(paper):
    """Creates elastic search document object based on pubmed schema for indexing"""
    doc = {
        "_index": index_name,
        "_id": paper.pm_id,
        "_source": {
            "pm_id": paper.pm_id,
            "title": paper.title,
            "abstract": paper.abstract,
            "created_date": paper.created_datetime,
            "authors": paper.authors,
            "mesh_annotations": paper.mesh_annotations,
            "references": paper.references,
        },
    }
    return doc


def index_pubmed_files(list_of_files, create_new_index=True):
    """Indexes all the gz files in the provided list_of_files parameter"""
    # print('Create pubmed paper es index... '),
    # create_new_index = True
    if create_new_index:
        create_pubmed_paper_index()
    # print('done')
    files_indexed = 0
    # Loop over all files, extract the information and index in bulk
    for i, f in enumerate(list_of_files):
        print(
            "Started processing file# %d of %d, %s"
            % (i + 1, len(list_of_files), os.path.basename(f))
        )
        time0 = time.time()
        time1 = time.time()
        inF = gzip.open(f, "rb")
        # we have to iterate through the subtrees, ET.parse() would result in memory issues
        context = ET.iterparse(inF, events=("start", "end"))
        # turn it into an iterator
        context = iter(context)

        # get the root element
        event, root = context.__next__()
        # print("Started extracting pubMed data from the file: %0.4fsec" % ((time.time() - time1)))
        time1 = time.time()

        rows_processed = 0
        rows_written = 0
        documents = []
        for event, elem in context:
            if event == "end" and elem.tag == "PubmedArticle":
                doc = PubmedArticle.extract_data(elem)
                rows_processed += 1
                if doc is not None:
                    # doc['pubmed_source_file'] = os.path.basename(f)
                    documents.append(doc)
                    rows_written += 1
                elem.clear()
        root.clear()
        print(
            "Finished extracting pubMed data from the file: %0.4fsec"
            % ((time.time() - time1))
        )
        print("Started Indexing into Elastic Search...")
        time1 = time.time()

        if len(documents) > 0:
            try:
                for ok, response in helpers.streaming_bulk(
                    es, documents, index=index_name, raise_on_error=True
                ):
                    if not ok:
                        # If the indexing is not successful, log the error
                        identifier = response["create"]["_id"]
                        error = response["create"]["error"]
                        print("Error processing document ID {}:".format(identifier))
                        print(json.dumps(error, indent=4, ensure_ascii=False))
            except Exception as e:
                traceback.print_exc()

        # es.indices.refresh(index=index_name)
        print(
            "Finished Indexing into Elastic Search: %0.4fsec"
            % ((time.time() - time1))
        )
        print(
            "Total time spend on this file %s: %0.4fsec"
            % (os.path.basename(f), (time.time() - time0))
        )
        print("Rows processed %d/%d\n" % (rows_written, rows_processed))
        files_indexed += 1
        # os.remove(f) #Uncomment if we need to delete file after processing
    return files_indexed


def index_all_pubmed_files(pubmed_folder):
    """Indexes all the gz files available in the provided pubmed_folder"""
    # pubmed_folder = '/Users/muhammadayub/Documents/PubmedData/test'
    print("Getting all .gz files from %s" % (pubmed_folder))
    # get a list of all .gz files in the provided folder
    list_of_files = get_all_gz_files(pubmed_folder)
    files_indexed = index_pubmed_files(list_of_files)
    return files_indexed


class PubmedProcessor:
    """This class is for handling pubmed data processing, extract
    pubmed data from xml and indexing into elastic search
    """


class PubmedArticle:
    """Represents Pubmed article object, holds data before indexing"""

    def __init__(self):
        self.pm_id = 0
        self.created_datetime = datetime.datetime.today()
        self.title = ""
        self.abstract = ""

    def __repr__(self):
        return "<Pubmed_paper %r>" % (self.pm_id)

    @staticmethod
    def extract_data(pubmed_article):
        """extract pubmed data for indexing from xml contained within <PubmedArticle> </PubmedArticle> elements"""
        # XML parsing based on following 2023 pubmed dtd:  https://dtd.nlm.nih.gov/ncbi/pubmed/doc/out/230101/el-MeshHeading.html
        new_pubmed_article = PubmedArticle()
        citation = pubmed_article.find("MedlineCitation")

        if citation is not None:
            new_pubmed_article.pm_id = citation.find("PMID").text
            new_pubmed_article.title = citation.find("Article/ArticleTitle").text

            created_date = pubmed_article.find('.//PubMedPubDate[@PubStatus="pubmed"]')
            if created_date is not None:
                new_pubmed_article.created_datetime = datetime.datetime(
                    int(created_date.find("Year").text),
                    int(created_date.find("Month").text),
                    int(created_date.find("Day").text),
                ).date()
            else:
                # Print an error message indicating the PMID for which DateCreated is not found in the XML
                print(
                    f"ERROR: No 'PubMedPubDate' element found for PMID {new_pubmed_article.pm_id}"
                )
                new_pubmed_article.created_datetime = (
                    None  # Set the value to None to indicate the missing data
                )

            current_date = datetime.datetime.now().date()
            time_difference = current_date - new_pubmed_article.created_datetime
            # check if the time difference is more than 5 years
            if time_difference.days >= 365 * 5:
                # print("Article(PMID:%s) date:%s is older than 5 years and is ignored for indexing" % (new_pubmed_paper.pm_id, new_pubmed_paper.created_datetime))
                return None

            abstract = citation.find("Article/Abstract")
            # abstract_dict = {"text":"", "background":"", "objective":"", "methods":"", "results":"", "conclusion":""}
            if abstract is not None:
                # Here we discart information about objectives, design, results and conclusion etc.
                for abstract_text in abstract.findall("AbstractText"):
                    if abstract_text.text:
                        if abstract_text.get("Label"):
                            new_pubmed_article.abstract += (
                                "<b>" + abstract_text.get("Label") + "</b>: "
                            )
                        new_pubmed_article.abstract += abstract_text.text + "<br>"

            # Author list
            authors = []
            for author in citation.findall("Article/AuthorList/Author"):
                author_dict = {}
                last_name = author.find("./LastName")
                if last_name is not None:
                    author_dict["last_name"] = last_name.text
                else:
                    author_dict["last_name"] = None
                fore_name = author.find("./ForeName")
                if fore_name is not None:
                    author_dict["first_name"] = fore_name.text
                else:
                    author_dict["first_name"] = None
                author_identifier = author.find("./Identifier[@Source='ORCID']")
                if author_identifier is not None:
                    author_dict["ORCID"] = author_identifier.text
                else:
                    author_dict["ORCID"] = ""
                affiliations = []
                for affiliation in author.findall("./AffiliationInfo/Affiliation"):
                    affiliations.append(affiliation.text)
                author_dict["affiliations"] = affiliations
                authors.append(author_dict)
            new_pubmed_article.authors = authors

            # Extract the MeSH annotations
            mesh_annotations = []
            for mesh_heading in citation.findall(".//MeshHeadingList/MeshHeading"):
                descriptor_name = mesh_heading.find("./DescriptorName").text
                descriptor_mesh_id = mesh_heading.find("./DescriptorName").attrib["UI"]
                descriptor_major_topic = mesh_heading.find("./DescriptorName").attrib[
                    "MajorTopicYN"
                ]

                qualifier_names = []
                for qn in mesh_heading.findall("./QualifierName"):
                    qualifier_name = qn.text
                    qualifier_mesh_id = qn.attrib["UI"]
                    qualifier_major_topic = qn.attrib["MajorTopicYN"]
                    qualifier_names.append(
                        {
                            "qualifier_name": qualifier_name,
                            "qualifier_mesh_id": qualifier_mesh_id,
                            "qualifier_major_topic": qualifier_major_topic,
                        }
                    )

                mesh_annotations.append(
                    {
                        "descriptor_name": descriptor_name,
                        "descriptor_mesh_id": descriptor_mesh_id,
                        "descriptor_major_topic": descriptor_major_topic,
                        "qualifier_names": qualifier_names,
                    }
                )
            new_pubmed_article.mesh_annotations = mesh_annotations

            pm_ids = []
            # extract the article references
            for reference in pubmed_article.findall(
                "./PubmedData/ReferenceList/Reference"
            ):
                for pm_id in reference.findall(
                    "ArticleIdList/ArticleId[@IdType='pubmed']"
                ):
                    pm_ids.append(pm_id.text)
            new_pubmed_article.references = pm_ids
            pubmed_processor = PubmedProcessor()
            doc = get_elasticsearch_doc(new_pubmed_article)
            del new_pubmed_article
        return doc
