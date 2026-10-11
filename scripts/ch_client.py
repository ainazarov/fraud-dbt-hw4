"""Small, dependency-free ClickHouse HTTP client used by the audit scripts."""

from __future__ import annotations

import base64
import json
import os
from decimal import Decimal
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urlsplit, urlunsplit
from urllib.request import Request, urlopen


SQL_DIR = Path(__file__).resolve().parents[1] / "sql"
QUERY_FILES = {
    "baseline": "002_max_by_state_baseline.sql",
    "raw": "003_max_by_state_raw.sql",
    "aggregate": "004_max_by_state.sql",
}


def read_query(name: str) -> str:
    """Return a SELECT without its trailing semicolon, ready for FORMAT."""
    return (SQL_DIR / QUERY_FILES[name]).read_text(encoding="utf-8").strip().rstrip(";")


class ClickHouseHTTP:
    def __init__(self, timeout: float = 120) -> None:
        self.url = os.environ.get("CLICKHOUSE_URL", "http://clickhouse:8123")
        self.user = os.environ.get("CLICKHOUSE_USER", "default")
        self.password = os.environ.get("CLICKHOUSE_PASSWORD", "hw2_local")
        self.timeout = timeout

    def query(
        self,
        sql: str,
        dataset: str | None = None,
        settings: dict[str, Any] | None = None,
        query_id: str | None = None,
        output_format: str | None = "JSONEachRow",
        parameters: dict[str, Any] | None = None,
    ) -> str:
        """Execute SQL via POST; bind values using ClickHouse typed parameters."""
        args: dict[str, Any] = {"wait_end_of_query": 1}
        if dataset is not None:
            args["param_dataset"] = dataset
        if parameters:
            args.update({f"param_{key}": value for key, value in parameters.items()})
        if settings:
            args.update(settings)
        if query_id is not None:
            args["query_id"] = query_id
        query = sql.strip().rstrip(";")
        if output_format:
            if output_format not in {"JSONEachRow", "CSVWithNames", "TabSeparatedRaw"}:
                raise ValueError(f"Unsupported output format: {output_format}")
            query += f"\nFORMAT {output_format}"
        url = urlsplit(self.url)
        query_string = "&".join(part for part in (url.query, urlencode(args)) if part)
        endpoint = urlunsplit((url.scheme, url.netloc, url.path or "/", query_string, ""))
        credentials = base64.b64encode(f"{self.user}:{self.password}".encode()).decode("ascii")
        request = Request(
            endpoint,
            data=query.encode("utf-8"),
            headers={"Authorization": f"Basic {credentials}", "Content-Type": "text/plain; charset=utf-8"},
            method="POST",
        )
        try:
            with urlopen(request, timeout=self.timeout) as response:
                return response.read().decode("utf-8")
        except HTTPError as exc:
            message = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"ClickHouse HTTP {exc.code}: {message[:4000]}") from exc
        except URLError as exc:
            raise RuntimeError(f"ClickHouse request failed: {exc.reason}") from exc

    def rows(self, sql: str, **kwargs: Any) -> list[dict[str, Any]]:
        return [
            json.loads(line, parse_float=Decimal)
            for line in self.query(sql, output_format="JSONEachRow", **kwargs).splitlines()
            if line.strip()
        ]
