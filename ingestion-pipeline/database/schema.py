import json
import os
from contextlib import contextmanager
from datetime import datetime
from io import BytesIO
import random
from numbers import Number
from typing import List, Tuple, Dict

from sqlalchemy import (
    create_engine,
    BigInteger,
    Column,
    String,
    ForeignKey,
    Integer,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Session, declarative_base, relationship

from pgcopy import CopyManager

from .models import AuthorInfo

Base = declarative_base()


@contextmanager
def engine_scope():
    db_url = os.environ.get("DOYEN_DB_URL")
    if not db_url:
        raise RuntimeError("Cannot load database models without DB URL.")

    engine = create_engine(db_url, connect_args={"sslmode": "prefer"})
    yield engine


@contextmanager
def session_scope():
    with engine_scope() as engine:
        with Session(engine) as session, session.begin():
            yield session


class Institution(Base):
    __tablename__ = "institution"

    identity_hash = Column(BigInteger, primary_key=True)
    name = Column(String, nullable=False)
    identifiers = Column(JSONB)


class InstitutionAuthorLink(Base):
    __tablename__ = "institution_author_link"

    institution_hash = Column(
        BigInteger,
        ForeignKey("institution.identity_hash"),
        nullable=False,
        primary_key=True,
    )
    author_hash = Column(
        BigInteger, ForeignKey("author.identity_hash"), nullable=False, primary_key=True
    )


class MeshAnnotation(Base):
    __tablename__ = "mesh_annotation"

    id = Column(Integer, primary_key=True)
    author_hash = Column(BigInteger, ForeignKey("author.identity_hash"), nullable=False)
    mesh_id = Column(Integer, ForeignKey("mesh_term.mesh_id"), nullable=False)


class MeshTerm(Base):
    __tablename__ = "mesh_term"

    mesh_id = Column(Integer, primary_key=True)
    mesh_name = Column(String)


class PaperLink(Base):
    __tablename__ = "paper_link"

    paper_id = Column(Integer, primary_key=True)
    author_hash = Column(
        BigInteger, ForeignKey("author.identity_hash"), primary_key=True
    )


class Author(Base):
    __tablename__ = "author"

    identity_hash = Column(BigInteger, primary_key=True)
    first_name = Column(String)
    last_name = Column(String, nullable=False)
    initials = Column(String)
    suffix = Column(String)
    identifier = Column(String)


class LazyCopyManager(CopyManager):
    """A copy manager that ignores entries which violate constraints."""

    _fill_tmp_fmt = (
        'CREATE TEMP TABLE "tmp_{table}"\n'
        "ON COMMIT DROP\n"
        'AS SELECT "{cols}" FROM "{schema}"."{table}"\n'
        "WITH NO DATA;\n"
        'COPY "tmp_{table}" ("{cols}")\n'
        "FROM STDIN WITH BINARY;"
    )

    _merge_fmt = (
        'INSERT INTO "{schema}"."{table}" ("{cols}")\n'
        'SELECT "{cols}"\n'
        'FROM "tmp_{table}" ON CONFLICT '
    )

    def __init__(self, conn, table, cols, constraint=None):
        super().__init__(conn, table, cols)
        self.constraint = constraint
        return

    @staticmethod
    def _stringify_cols(cols):
        if not isinstance(cols, list) and not isinstance(cols, tuple):
            raise ValueError(
                f"Argument `cols` must be a list or tuple, got: " f"{type(cols)}"
            )
        return '", "'.join(cols)

    def _fmt_sql(self, sql_fmt):
        columns = self._stringify_cols(self.cols)
        sql = sql_fmt.format(schema=self.schema, table=self.table, cols=columns)
        return sql

    def _get_insert_sql(self):
        cmd_fmt = self._merge_fmt
        if self.constraint:
            cmd_fmt += 'ON CONSTRAINT "%s" ' % self.constraint
        cmd_fmt += "DO NOTHING;\n"
        return self._fmt_sql(cmd_fmt)

    def _get_copy_sql(self):
        return self._fmt_sql(self._fill_tmp_fmt)

    def _get_sql(self):
        return "\n".join([self._get_copy_sql(), self._get_insert_sql()])

    def _get_skipped(self, num, order_by, return_cols=None):
        cursor = self.conn.cursor()
        inp_cols = self._stringify_cols(self.cols)
        if return_cols:
            ret_cols = self._stringify_cols(return_cols)
        else:
            ret_cols = inp_cols
        diff_sql = (
            f'SELECT "{ret_cols}" FROM\n'
            f'(SELECT "{inp_cols}" FROM "tmp_{self.table}"\n'
            f" EXCEPT\n"
            f' (SELECT "{inp_cols}"\n'
            f'  FROM "{self.schema}"."{self.table}"\n'
            f'  ORDER BY "{order_by}" DESC\n'
            f"  LIMIT {num})) as t;"
        )
        cursor.execute(diff_sql)
        res = cursor.fetchall()
        return res

    def copystream(self, datastream):
        sql = self._get_sql()

        cursor = self.conn.cursor()
        try:
            cursor.copy_expert(sql, datastream)
        except Exception as e:
            templ = "error doing lazy binary copy into {0}.{1}:\n{2}"
            e.message = templ.format(self.schema, self.table, e)
            raise e
        return


def make_copy_batch_id():
    """Generate a random batch id for copying into the database.
    This allows for easy retrieval of the assigned ids immediately after
    copying in. At this time, only Reading and RawStatements use the
    feature.
    """
    return random.randint(-(2**30), 2**30)


def _prep_copy(tbl, data, cols):
    # If cols is not specified, use all the cols in the table, else check
    # to make sure the names are valid.
    all_cols = list(Base.metadata.tables[tbl.__tablename__].columns.keys())
    if cols is None:
        cols = all_cols
    else:
        assert all(
            [col in all_cols for col in cols]
        ), "Do not recognize one of the columns in %s for table %s." % (
            cols,
            tbl.__tablename__,
        )

    # Format the data for the copy.
    data_bts = []
    n_cols = len(cols)
    for entry in data:
        # Make sure that the number of columns matches the number of columns
        # in the data.
        if n_cols != len(entry):
            raise ValueError(
                "Number of columns does not match number of columns in data."
            )

        # Convert the entry to bytes
        new_entry = []
        for element in entry:
            if isinstance(element, str):
                new_entry.append(element.encode("utf8"))
            elif isinstance(element, dict) or isinstance(element, list):
                new_entry.append(json.dumps(element).encode("utf-8"))
            elif (
                isinstance(element, bytes)
                or element is None
                or isinstance(element, Number)
                or isinstance(element, datetime)
            ):
                new_entry.append(element)
            else:
                raise ValueError(
                    f"Don't know what to do with element of type {type(element)}. "
                    "Should be str, bytes, datetime, None, or a number."
                )
        data_bts.append(tuple(new_entry))

    return cols, data_bts


def copy_lazy(tbl, data, cols=None):
    """Copy lazily, skip any rows that violate constraints."""
    # General overhead.
    if len(data) == 0:
        return
    cols, data_bts = _prep_copy(tbl, data, cols)

    with engine_scope() as engine:
        # Do the copy.
        conn = engine.raw_connection()
        mngr = LazyCopyManager(conn, tbl.__tablename__, cols)
        mngr.copy(data_bts, BytesIO)
        conn.commit()

    return


def get_mesh_int(mesh_id: str) -> int:
    return int(mesh_id.replace("D", "1").replace("C", "2"))


def batch_update(
    authors: List[AuthorInfo], mesh_terms: Dict[int, Dict], papers: Dict[int, List]
):
    author_rows = [
        (
            a.get_durable_hash(),
            a.last_name,
            a.first_name,
            a.initials,
            a.suffix,
            a.identifier,
        )
        for a in authors
    ]
    print(f"Uploading {len(author_rows)} authors.")
    copy_lazy(
        Author,
        author_rows,
        [
            "identity_hash",
            "last_name",
            "first_name",
            "initials",
            "suffix",
            "identifier",
        ],
    )

    unique_institutions = {inst for auth in authors for inst in auth.affiliations}

    print(f"Adding {len(unique_institutions)} institutions")
    copy_lazy(
        Institution,
        [
            (inst.get_durable_hash(), inst.name, inst.identifiers)
            for inst in unique_institutions
        ],
        ["identity_hash", "name", "identifiers"],
    )

    institution_links = {
        (inst.get_durable_hash(), auth.get_durable_hash())
        for auth in authors
        for inst in auth.affiliations
    }
    print(f"Adding {len(institution_links)} institution links")
    copy_lazy(
        InstitutionAuthorLink, institution_links, ["institution_hash", "author_hash"]
    )

    unique_mesh_terms = {
        (get_mesh_int(mesh_id), mesh_term)
        for mesh_links in mesh_terms.values()
        for mesh_id, mesh_term in mesh_links.keys()
    }
    print(f"Adding {len(unique_mesh_terms)} unique mesh terms.")
    copy_lazy(MeshTerm, list(unique_mesh_terms), ["mesh_id", "mesh_name"])

    mesh_links = [
        (get_mesh_int(mesh_id), auth_id)
        for auth_id, mesh_links in mesh_terms.items()
        for (mesh_id, _), cnt in mesh_links.items()
        for _ in range(cnt)
    ]
    print(f"Adding {len(mesh_links)} mesh links.")
    copy_lazy(MeshAnnotation, mesh_links, ["mesh_id", "author_hash"])

    paper_links = [
        (int(paper_id), auth_id)
        for auth_id, paper_ids in papers.items()
        for paper_id in paper_ids
    ]
    print(f"Adding {len(paper_links)} paper links.")
    copy_lazy(
        PaperLink,
        paper_links,
        ["paper_id", "author_hash"],
    )
