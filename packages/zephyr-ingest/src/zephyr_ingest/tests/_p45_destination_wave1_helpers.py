from __future__ import annotations

import hashlib
import hmac
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import cast
from urllib.parse import quote, urlsplit
from xml.etree import ElementTree

import httpx

from zephyr_core import DocumentRef, RunContext
from zephyr_core.contracts.v1.enums import PartitionStrategy
from zephyr_core.contracts.v1.models import (
    DocumentMetadata,
    EngineInfo,
    PartitionResult,
    ZephyrElement,
)
from zephyr_ingest._internal.utils import sha256_file
from zephyr_ingest.destinations.base import Destination
from zephyr_ingest.flow_processor import normalize_flow_input_identity_sha
from zephyr_ingest.queue_backend_factory import build_local_queue_backend
from zephyr_ingest.runner import RunnerConfig, build_task_execution_handler, run_documents
from zephyr_ingest.task_v1 import (
    TaskDocumentInputV1,
    TaskExecutionV1,
    TaskIdentityV1,
    TaskInputsV1,
    TaskV1,
)
from zephyr_ingest.worker_runtime import run_worker


def _json_object_from_text(text: str) -> dict[str, object]:
    raw: object = json.loads(text)
    assert isinstance(raw, dict)
    return cast(dict[str, object], raw)


def read_json_dict(path: Path) -> dict[str, object]:
    return _json_object_from_text(path.read_text(encoding="utf-8"))


def as_dict(value: object) -> dict[str, object]:
    assert isinstance(value, dict)
    return cast(dict[str, object], value)


def make_text_document(*, root: Path, name: str = "doc.txt", text: str) -> DocumentRef:
    path = root / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    return DocumentRef(
        uri=str(path),
        source="local_file",
        discovered_at_utc="2026-04-15T00:00:00Z",
        filename=path.name,
        extension=path.suffix.lower(),
        size_bytes=path.stat().st_size,
    )


class StaticTextProcessor:
    def process(
        self,
        *,
        doc: DocumentRef,
        strategy: PartitionStrategy,
        unique_element_ids: bool,
        run_id: str | None,
        pipeline_version: str | None,
        sha256: str,
    ) -> PartitionResult:
        del run_id, pipeline_version
        text = Path(doc.uri).read_text(encoding="utf-8")
        return PartitionResult(
            document=DocumentMetadata(
                filename=doc.filename or Path(doc.uri).name,
                mime_type="text/plain",
                sha256=sha256,
                size_bytes=doc.size_bytes or 0,
                created_at_utc="2026-04-15T00:00:00Z",
            ),
            engine=EngineInfo(
                name="p45-static-processor",
                backend="local",
                version="1.0",
                strategy=strategy,
            ),
            elements=[
                ZephyrElement(
                    element_id="e1" if unique_element_ids else "0",
                    type="Text",
                    text=text,
                    metadata={"uri": doc.uri},
                )
            ],
            normalized_text=text,
            warnings=[],
        )


def expected_uns_sha(doc: DocumentRef) -> str:
    return normalize_flow_input_identity_sha(
        flow_kind="uns",
        filename=doc.uri,
        default_sha=sha256_file(Path(doc.uri)),
    )


@dataclass(frozen=True, slots=True)
class BatchRunArtifacts:
    doc: DocumentRef
    out_root: Path
    sha256: str
    batch_report: dict[str, object]


@dataclass(frozen=True, slots=True)
class WorkerRunArtifacts:
    doc: DocumentRef
    out_root: Path
    sha256: str


def _ctx(*, run_id: str, pipeline_version: str = "p45-m2-wave1") -> RunContext:
    return RunContext.new(
        pipeline_version=pipeline_version,
        run_id=run_id,
        timestamp_utc="2026-04-15T00:00:00Z",
    )


def run_batch_case(
    *,
    tmp_root: Path,
    destination: Destination,
    run_id: str,
    text: str,
    force: bool = False,
) -> BatchRunArtifacts:
    doc = make_text_document(root=tmp_root / "docs", text=text)
    out_root = tmp_root / "out-batch"
    run_documents(
        docs=[doc],
        cfg=RunnerConfig(out_root=out_root, destination=destination, force=force),
        ctx=_ctx(run_id=run_id),
        flow_kind="uns",
        processor=StaticTextProcessor(),
    )
    return BatchRunArtifacts(
        doc=doc,
        out_root=out_root,
        sha256=expected_uns_sha(doc),
        batch_report=read_json_dict(out_root / "batch_report.json"),
    )


def run_worker_case(
    *,
    tmp_root: Path,
    destination: Destination,
    run_id: str,
    doc: DocumentRef,
) -> WorkerRunArtifacts:
    out_root = tmp_root / "out-worker"
    sha256 = expected_uns_sha(doc)
    queue_backend = build_local_queue_backend(kind="sqlite", root=tmp_root / "queue-worker")
    queue_backend.enqueue(
        TaskV1(
            task_id=f"task-{run_id}",
            kind="uns",
            inputs=TaskInputsV1(document=TaskDocumentInputV1.from_document_ref(doc)),
            execution=TaskExecutionV1(
                strategy=PartitionStrategy.AUTO,
                unique_element_ids=True,
            ),
            identity=TaskIdentityV1(
                pipeline_version="p45-m2-wave1",
                sha256=sha256,
            ),
        )
    )
    worker_ctx = _ctx(run_id=run_id)
    rc = run_worker(
        ctx=worker_ctx,
        poll_interval_ms=1,
        queue_backend=queue_backend,
        task_handler=build_task_execution_handler(
            cfg=RunnerConfig(out_root=out_root, destination=destination),
            ctx=worker_ctx,
            flow_kind="uns",
            processor=StaticTextProcessor(),
        ),
        drain_on_empty=True,
        sleep_fn=lambda _: None,
    )
    assert rc == 0
    return WorkerRunArtifacts(doc=doc, out_root=out_root, sha256=sha256)


def webhook_reset(*, base_url: str) -> None:
    httpx.post(f"{base_url.rstrip('/')}/_reset", timeout=5.0, trust_env=False).raise_for_status()


def webhook_events(*, base_url: str) -> list[dict[str, object]]:
    response = httpx.get(f"{base_url.rstrip('/')}/_events", timeout=5.0, trust_env=False)
    response.raise_for_status()
    payload = _json_object_from_text(response.text)
    items_obj: object = payload.get("items")
    assert isinstance(items_obj, list)
    items = cast(list[object], items_obj)
    events: list[dict[str, object]] = []
    for item_obj in items:
        if isinstance(item_obj, dict):
            events.append(cast(dict[str, object], item_obj))
    return events


class S3ServiceError(Exception):
    def __init__(self, *, status_code: int, code: str, message: str) -> None:
        super().__init__(message)
        self.response: dict[str, object] = {
            "ResponseMetadata": {"HTTPStatusCode": status_code},
            "Error": {"Code": code, "Message": message},
        }


class SigV4MinioClient:
    def __init__(self, *, endpoint_url: str, access_key: str, secret_key: str, region: str) -> None:
        self.endpoint_url = endpoint_url.rstrip("/")
        self.access_key = access_key
        self.secret_key = secret_key
        self.region = region

    def _signature_key(self, datestamp: str) -> bytes:
        k_date = hmac.new(
            f"AWS4{self.secret_key}".encode("utf-8"),
            datestamp.encode("utf-8"),
            hashlib.sha256,
        ).digest()
        k_region = hmac.new(k_date, self.region.encode("utf-8"), hashlib.sha256).digest()
        k_service = hmac.new(k_region, b"s3", hashlib.sha256).digest()
        return hmac.new(k_service, b"aws4_request", hashlib.sha256).digest()

    def _request(
        self,
        *,
        method: str,
        bucket: str,
        key: str = "",
        params: dict[str, str] | None = None,
        body: bytes = b"",
        content_type: str = "application/octet-stream",
    ) -> httpx.Response:
        now = datetime.now(UTC)
        amz_date = now.strftime("%Y%m%dT%H%M%SZ")
        datestamp = now.strftime("%Y%m%d")
        host = urlsplit(self.endpoint_url).netloc
        canonical_uri = f"/{quote(bucket, safe='')}"
        if key:
            canonical_uri += f"/{quote(key, safe='/._-~')}"
        query_items = [] if params is None else sorted(params.items())
        canonical_query = "&".join(
            f"{quote(name, safe='-_.~')}={quote(value, safe='-_.~')}" for name, value in query_items
        )
        payload_hash = hashlib.sha256(body).hexdigest()
        canonical_headers = (
            f"content-type:{content_type}\n"
            f"host:{host}\n"
            f"x-amz-content-sha256:{payload_hash}\n"
            f"x-amz-date:{amz_date}\n"
        )
        signed_headers = "content-type;host;x-amz-content-sha256;x-amz-date"
        canonical_request = "\n".join(
            (
                method,
                canonical_uri,
                canonical_query,
                canonical_headers,
                signed_headers,
                payload_hash,
            )
        )
        scope = f"{datestamp}/{self.region}/s3/aws4_request"
        string_to_sign = "\n".join(
            (
                "AWS4-HMAC-SHA256",
                amz_date,
                scope,
                hashlib.sha256(canonical_request.encode("utf-8")).hexdigest(),
            )
        )
        signature = hmac.new(
            self._signature_key(datestamp),
            string_to_sign.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()
        headers = {
            "Authorization": (
                "AWS4-HMAC-SHA256 "
                f"Credential={self.access_key}/{scope}, "
                f"SignedHeaders={signed_headers}, Signature={signature}"
            ),
            "Content-Type": content_type,
            "Host": host,
            "X-Amz-Content-Sha256": payload_hash,
            "X-Amz-Date": amz_date,
        }
        with httpx.Client(timeout=10.0, trust_env=False) as client:
            return client.request(
                method,
                f"{self.endpoint_url}{canonical_uri}",
                params=params,
                headers=headers,
                content=body,
            )

    def _raise_for_status(self, response: httpx.Response) -> None:
        if 200 <= response.status_code < 300:
            return
        code = f"HTTP{response.status_code}"
        message = response.text[:500]
        try:
            root = ElementTree.fromstring(response.text)
        except ElementTree.ParseError:
            pass
        else:
            code_node = root.find("./Code")
            message_node = root.find("./Message")
            if code_node is not None and code_node.text is not None:
                code = code_node.text
            if message_node is not None and message_node.text is not None:
                message = message_node.text
        raise S3ServiceError(status_code=response.status_code, code=code, message=message)

    def ensure_bucket(self, *, bucket: str) -> None:
        response = self._request(method="PUT", bucket=bucket)
        if response.status_code in (200, 204):
            return
        try:
            self._raise_for_status(response)
        except S3ServiceError as exc:
            error_obj: object = exc.response.get("Error")
            if isinstance(error_obj, dict):
                error = cast(dict[str, object], error_obj)
                if error.get("Code") in {"BucketAlreadyOwnedByYou", "BucketAlreadyExists"}:
                    return
            raise

    def put_object(
        self, *, Bucket: str, Key: str, Body: bytes, ContentType: str
    ) -> dict[str, object]:
        response = self._request(
            method="PUT",
            bucket=Bucket,
            key=Key,
            body=Body,
            content_type=ContentType,
        )
        self._raise_for_status(response)
        details: dict[str, object] = {"ResponseMetadata": {"HTTPStatusCode": response.status_code}}
        etag = response.headers.get("etag")
        if etag is not None:
            details["ETag"] = etag
        return details

    def get_object_text(self, *, bucket: str, key: str) -> str:
        response = self._request(method="GET", bucket=bucket, key=key)
        self._raise_for_status(response)
        return response.text

    def list_keys(self, *, bucket: str, prefix: str) -> tuple[str, ...]:
        response = self._request(
            method="GET",
            bucket=bucket,
            params={"list-type": "2", "prefix": prefix},
        )
        self._raise_for_status(response)
        root = ElementTree.fromstring(response.text)
        return tuple(
            node.text for node in root.findall(".//{*}Contents/{*}Key") if node.text is not None
        )
