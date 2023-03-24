from hashlib import md5
from typing import List
from pydantic import BaseModel


class _HashableModel(BaseModel):
    durable_hash: int | None

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.durable_hash = None

    def identity_tuple(self):
        raise NotImplementedError()

    def __hash__(self):
        return hash(str(self.identity_tuple()))

    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            raise ValueError(
                f"Cannot compare equality between type {type(other)} and type {self.__class__}."
            )
        return self.identity_tuple() == other.identity_tuple()

    def get_durable_hash(self):
        if not self.durable_hash:
            self.durable_hash = int(
                md5(str(self.identity_tuple()).encode("utf-8")).hexdigest()[:15], 16
            )
        return self.durable_hash


class InstitutionInfo(_HashableModel):
    name: str
    identifiers: List[str] | None

    def identity_tuple(self):
        return self.name, sorted(self.identifiers)


class AuthorInfo(_HashableModel):
    first_name: str | None
    last_name: str
    initials: str | None
    suffix: str | None
    identifier: str | None
    affiliations: List[InstitutionInfo]

    def identity_tuple(self):
        return (
            self.first_name,
            self.last_name,
            self.initials,
            self.suffix,
            self.identifier,
            sorted(inst.identity_tuple() for inst in self.affiliations),
        )
