from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Literal, NotRequired, TypedDict, cast

from zephyr_core import DocumentRef, PartitionStrategy

TaskKind = Literal["uns", "it"]


class TaskDocumentInputV1Dict(TypedDict):
    uri: str
    source: str
    source_contract_id: NotRequired[str]
    discovered_at_utc: str
    filename: str
    extension: str
    size_bytes: int


class TaskInputsV1Dict(TypedDict):
    document: TaskDocumentInputV1Dict


class TaskExecutionV1Dict(TypedDict):
    strategy: str
    unique_element_ids: bool


class TaskIdentityV1Dict(TypedDict):
    pipeline_version: str
    sha256: str


class TaskV1Dict(TypedDict):
    task_id: str
    kind: TaskKind
    inputs: TaskInputsV1Dict
    execution: TaskExecutionV1Dict
    identity: NotRequired[TaskIdentityV1Dict]


def _require_mapping(*, data: Mapping[str, object], key: str) -> Mapping[str, object]:
    value = data[key]
    if not isinstance(value, Mapping):
        raise TypeError(f"TaskV1 field '{key}' must be a mapping")
    return cast("Mapping[str, object]", value)


def _require_str(*, data: Mapping[str, object], key: str) -> str:
    value = data[key]
    if not isinstance(value, str):
        raise TypeError(f"TaskV1 field '{key}' must be a string")
    return value


def _require_bool(*, data: Mapping[str, object], key: str) -> bool:
    value = data[key]
    if not isinstance(value, bool):
        raise TypeError(f"TaskV1 field '{key}' must be a boolean")
    return value


def _require_int(*, data: Mapping[str, object], key: str) -> int:
    value = data[key]
    if not isinstance(value, int) or isinstance(value, bool):
        raise TypeError(f"TaskV1 field '{key}' must be an integer")
    return value


def _parse_task_kind(value: object) -> TaskKind:
    if value == "uns":
        return "uns"
    if value == "it":
        return "it"
    raise ValueError(f"Unsupported task kind: {value!r}")


@dataclass(frozen=True, slots=True)
class TaskDocumentInputV1:
    uri: str
    source: str
    discovered_at_utc: str
    filename: str
    extension: str
    size_bytes: int
    source_contract_id: str | None = None

    @classmethod
    def from_document_ref(
        cls, doc: DocumentRef, *, source_contract_id: str | None = None
    ) -> TaskDocumentInputV1:
        return cls(
            uri=doc.uri,
            source=doc.source,
            discovered_at_utc=doc.discovered_at_utc,
            filename=doc.filename,
            extension=doc.extension,
            size_bytes=doc.size_bytes,
            source_contract_id=source_contract_id,
        )

    def to_document_ref(self) -> DocumentRef:
        return DocumentRef(
            uri=self.uri,
            source=self.source,
            discovered_at_utc=self.discovered_at_utc,
            filename=self.filename,
            extension=self.extension,
            size_bytes=self.size_bytes,
        )

    def to_dict(self) -> TaskDocumentInputV1Dict:
        payload: TaskDocumentInputV1Dict = {
            "uri": self.uri,
            "source": self.source,
            "discovered_at_utc": self.discovered_at_utc,
            "filename": self.filename,
            "extension": self.extension,
            "size_bytes": self.size_bytes,
        }
        if self.source_contract_id is not None:
            payload["source_contract_id"] = self.source_contract_id
        return payload

    @classmethod
    def from_dict(cls, data: Mapping[str, object]) -> TaskDocumentInputV1:
        source_contract_id_obj = data.get("source_contract_id")
        source_contract_id = None
        if source_contract_id_obj is not None:
            if not isinstance(source_contract_id_obj, str):
                raise TypeError("TaskV1 document field 'source_contract_id' must be a string")
            source_contract_id = source_contract_id_obj
        return cls(
            uri=_require_str(data=data, key="uri"),
            source=_require_str(data=data, key="source"),
            discovered_at_utc=_require_str(data=data, key="discovered_at_utc"),
            filename=_require_str(data=data, key="filename"),
            extension=_require_str(data=data, key="extension"),
            size_bytes=_require_int(data=data, key="size_bytes"),
            source_contract_id=source_contract_id,
        )


@dataclass(frozen=True, slots=True)
class TaskInputsV1:
    document: TaskDocumentInputV1

    def to_dict(self) -> TaskInputsV1Dict:
        return {"document": self.document.to_dict()}

    @classmethod
    def from_dict(cls, data: Mapping[str, object]) -> TaskInputsV1:
        document = TaskDocumentInputV1.from_dict(_require_mapping(data=data, key="document"))
        return cls(document=document)


@dataclass(frozen=True, slots=True)
class TaskExecutionV1:
    strategy: PartitionStrategy = PartitionStrategy.AUTO
    unique_element_ids: bool = True

    def to_dict(self) -> TaskExecutionV1Dict:
        return {
            "strategy": str(self.strategy),
            "unique_element_ids": self.unique_element_ids,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, object]) -> TaskExecutionV1:
        try:
            strategy = PartitionStrategy(_require_str(data=data, key="strategy"))
        except ValueError as exc:
            raise ValueError(f"Unsupported task strategy: {data['strategy']!r}") from exc

        return cls(
            strategy=strategy,
            unique_element_ids=_require_bool(data=data, key="unique_element_ids"),
        )


@dataclass(frozen=True, slots=True)
class TaskIdentityV1:
    pipeline_version: str
    sha256: str

    def to_dict(self) -> TaskIdentityV1Dict:
        return {
            "pipeline_version": self.pipeline_version,
            "sha256": self.sha256,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, object]) -> TaskIdentityV1:
        return cls(
            pipeline_version=_require_str(data=data, key="pipeline_version"),
            sha256=_require_str(data=data, key="sha256"),
        )


@dataclass(frozen=True, slots=True)
class TaskV1:
    task_id: str
    kind: TaskKind
    inputs: TaskInputsV1
    execution: TaskExecutionV1
    identity: TaskIdentityV1 | None = None

    def to_dict(self) -> TaskV1Dict:
        payload: TaskV1Dict = {
            "task_id": self.task_id,
            "kind": self.kind,
            "inputs": self.inputs.to_dict(),
            "execution": self.execution.to_dict(),
        }
        if self.identity is not None:
            payload["identity"] = self.identity.to_dict()
        return payload

    @classmethod
    def from_dict(cls, data: Mapping[str, object]) -> TaskV1:
        identity_data = data.get("identity")
        identity = None
        if identity_data is not None:
            if not isinstance(identity_data, Mapping):
                raise TypeError("TaskV1 field 'identity' must be a mapping")
            identity = TaskIdentityV1.from_dict(cast("Mapping[str, object]", identity_data))

        return cls(
            task_id=_require_str(data=data, key="task_id"),
            kind=_parse_task_kind(data["kind"]),
            inputs=TaskInputsV1.from_dict(_require_mapping(data=data, key="inputs")),
            execution=TaskExecutionV1.from_dict(_require_mapping(data=data, key="execution")),
            identity=identity,
        )
