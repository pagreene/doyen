import logging
from datetime import datetime
from typing import List, Any, Tuple, Dict

import gilda
from pydantic import BaseModel

from elasticsearch import logger as es_logger
from ingestion_pipeline.pubmed_processor import get_es_client
import networkx as nx
from networkx.algorithms.components import connected_components


logging.basicConfig(
    level=logging.WARNING,
    format=f"[%(asctime)s] %(name)s %(levelname)s - %(message)s"
)

# Set the logging level of the elasticsearch module to WARNING
es_logger.setLevel(logging.WARNING)


class Affiliation(BaseModel):
    name: str
    identifiers: List[Any]


class Author(BaseModel):
    first_name: str
    last_name: str
    initials: str | None
    suffix: str | None
    affiliations: List[Affiliation]
    identifier: str | None

    papers: List["Paper"] = None

    def key(self):
        return f"{self.first_name} {self.last_name}"

    def score(self):
        """Return the score of this author."""
        return sum(p.score for p in self.papers)


class Paper(BaseModel):
    pmid: str
    title: str
    authors: List[Author] = None
    mesh_annotations: List[dict]
    score: float

    def key(self):
        return f"{self.pmid} {self.title}"


class Search:
    def __init__(self, *search_terms: str, max_papers=100):
        self.search_terms = search_terms
        self.authors = None
        self.papers = None
        self.graph = None

        self.__search(max_papers=max_papers)

    def __search(self, max_papers=100):
        """Search for papers and authors matching the given terms."""
        # Convert the terms we can to MeSH
        print("Grounding terms...", end="", flush=True)
        start_grounding = datetime.now()
        match_query_terms = []
        for term in self.search_terms:
            scored_matches = gilda.ground(term, namespaces=["MESH"])

            if scored_matches:
                match_query_terms.append(
                    {"match": {"mesh_annotations.mesh": scored_matches[0].term.id}}
                )
            else:
                match_query_terms.append({"match": {"abstract": term}})
        print(f"done in {datetime.now() - start_grounding} seconds", flush=True)

        # Run the search against ElasticSearch
        print("Searching for papers...", end="", flush=True)
        start_search = datetime.now()
        es = get_es_client()
        resp = es.search(
            index="pubmed-paper-index",
            query={"bool": {"should": match_query_terms}},
            size=max_papers,
        )
        print(f"done in {datetime.now() - start_search} seconds", flush=True)

        # Convert the results into models for authors and papers
        print("Building models...", end="", flush=True)
        start_models = datetime.now()
        self.authors = {}
        self.papers = {}
        for hit in resp["hits"]["hits"]:
            paper = Paper(
                pmid=hit["_source"]["pmid"],
                title=hit["_source"]["title"],
                score=hit["_score"],
                mesh_annotations=hit["_source"]["mesh_annotations"],
                authors=[],
            )
            for author_info in hit["_source"]["authors"]:
                author = Author(**author_info)
                if author.key() not in self.authors:
                    author.papers = []
                    self.authors[author.key()] = author
                else:
                    author = self.authors[author.key()]
                author.papers.append(paper)
                paper.authors.append(author)

            self.papers[paper.key()] = paper
        print(f"done in {datetime.now() - start_models} seconds", flush=True)

        # Build the graph
        print("Building graph...", end="", flush=True)
        start_graph = datetime.now()
        self.graph = nx.Graph()
        for author in self.authors.values():
            for paper in author.papers:
                self.graph.add_edge(author.key(), paper.key())
        print(f"done in {datetime.now() - start_graph} seconds", flush=True)

        return

    def iter_subgraphs(self):
        """Iterate over the subgraphs of the graph."""
        for subgraph in connected_components(self.graph):
            yield self.graph.subgraph(subgraph)

    def plot_subgraph(self, graph, ax):
        pos = nx.spring_layout(graph, k=0.5)
        author_nodes = [a for a in graph.nodes if a in self.authors]
        nx.draw_networkx_nodes(
            graph,
            pos,
            nodelist=author_nodes,
            node_color="tab:red",
            node_size=[100*len(self.authors[a].papers) for a in author_nodes],
            ax=ax,
        )
        paper_nodes = [p for p in graph.nodes if p in self.papers]
        nx.draw_networkx_nodes(
            graph,
            pos,
            nodelist=paper_nodes,
            node_color="tab:blue",
            node_size=[500*self.papers[p] for p in paper_nodes],
            ax=ax,
        )
        nx.draw_networkx_edges(graph, pos, ax=ax)
