from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field


class KagiRelatedSearch(BaseModel):
    model_config = ConfigDict(extra="ignore", strict=True)

    title: str = Field(min_length=1)


class KagiSearchImage(BaseModel):
    model_config = ConfigDict(extra="ignore", strict=True)

    url: str = Field(min_length=1)


class KagiSearchResult(BaseModel):
    model_config = ConfigDict(extra="ignore", strict=True)

    url: str = Field(min_length=1)
    title: str = Field(min_length=1)
    snippet: str | None = None
    image: KagiSearchImage | None = None


class KagiSearchData(BaseModel):
    model_config = ConfigDict(extra="ignore", strict=True)

    related_search: list[KagiRelatedSearch] = Field(default_factory=list)
    search: list[KagiSearchResult] = Field(default_factory=list)


class KagiSearchMeta(BaseModel):
    model_config = ConfigDict(extra="ignore", strict=True)
    ms: int = Field(ge=0)


class KagiSearchResponse(BaseModel):
    model_config = ConfigDict(extra="ignore", strict=True)

    data: KagiSearchData
    meta: KagiSearchMeta

    def to_result_json(self) -> str:
        return self.model_dump_json(exclude_none=True)
