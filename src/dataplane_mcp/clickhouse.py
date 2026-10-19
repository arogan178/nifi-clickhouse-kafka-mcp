import json
import uuid

import requests

from .config import ClickHouseConfig
from .masking import MASK, mask

# Statements the gateway runs, by the root node of ClickHouse's own parse (EXPLAIN AST).
READ_ROOTS = ("SelectWithUnionQuery", "SelectIntersectExceptQuery", "Explain", "Show", "DescribeQuery", "Exists")
# Table functions that only read the server itself; everything else (url, s3, file, remote, mysql, ...) could fetch
# or exfiltrate data, and new ones appear every release, so this is an allowlist.
INTERNAL_TABLE_FUNCTIONS = frozenset(
    {
        "numbers",
        "numbers_mt",
        "zeros",
        "zeros_mt",
        "generaterandom",
        "generate_series",
        "generateseries",
        "values",
        "view",
        "viewifpermitted",
        "merge",
        "cluster",
        "clusterallreplicas",
        "null",
        "loop",
        "mergetreeindex",
        "mergetreeprojection",
        "primes",
        "viewexplain",
        # Also the everyday scalar format('{} {}', a, b); as a table function it only parses inline data.
        "format",
    }
)
# Used when system.table_functions can't be read; the server's own list is preferred because it grows.
KNOWN_TABLE_FUNCTIONS = INTERNAL_TABLE_FUNCTIONS | frozenset(
    """url urlcluster s3 s3cluster gcs oss cosn azureblobstorage azureblobstoragecluster hdfs hdfscluster file
    filecluster remote remotesecure mysql postgresql mongodb jdbc odbc sqlite redis executable input iceberg icebergs3
    icebergs3cluster icebergazure icebergazurecluster icebergHDFS iceberglocal deltalake deltalakes3 deltalakeazure
    deltalakelocal hudi hive arrowflight ytsaurus paimon paimons3 paimonazure paimonhdfs paimonlocal dictionary
    fuzzjson fuzzquery format generaterandomstructure""".lower().split()
)
INTERNAL_ROWS = 100_000


def check_ast(
    ast: str,
    allowed_functions: frozenset[str] = INTERNAL_TABLE_FUNCTIONS,
    table_functions: frozenset[str] = KNOWN_TABLE_FUNCTIONS,
) -> None:
    """Refuse a statement whose EXPLAIN AST is not a read, or that names a table function not allowed anywhere,
    including inside another function's arguments, such as cluster('c', url(...)), or as a scalar such as file()."""
    lines = ast.splitlines()
    root = lines[0].split(None, 1)[0] if lines else ""
    # EXPLAIN wraps another statement; that one must be a read too.
    depth = 0
    while root == "Explain" and len(lines) > depth + 1:
        depth += 1
        root = lines[depth].split(None, 1)[0]
    if not root.startswith(READ_ROOTS):
        raise ValueError(f"only SELECT, WITH, SHOW, DESCRIBE, EXISTS and EXPLAIN statements are allowed, not {root}")
    refused = set()
    for i, line in enumerate(lines):
        words = line.split()
        if words[:1] == ["Function"] and words[1].lower() in table_functions - allowed_functions:
            refused.add(words[1])
        if words[:1] != ["TableExpression"]:
            continue
        # Whatever stands in the FROM position must be allowed, even if the server's list doesn't know it.
        indent = len(line) - len(line.lstrip())
        for child in lines[i + 1 :]:
            child_indent = len(child) - len(child.lstrip())
            if child_indent <= indent:
                break
            child_words = child.split()
            if child_indent == indent + 1 and child_words[0] == "Function":
                if child_words[1].lower() not in allowed_functions:
                    refused.add(child_words[1])
    if refused:
        raise ValueError(
            f"table function {', '.join(sorted(refused))} is not allowed; only functions that read ClickHouse itself "
            "are, unless the server config adds them to allowed_table_functions"
        )


def param_value(value) -> str:
    """A query parameter in ClickHouse's text format: strings escaped, NULL as \\N, arrays as literals."""
    if value is None:
        return "\\N"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, str):
        return value.replace("\\", "\\\\").replace("\t", "\\t").replace("\n", "\\n")
    if isinstance(value, list):
        return "[" + ",".join(_literal(v) for v in value) + "]"
    return str(value)


def _literal(value) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, str):
        return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"
    return param_value(value)


def _exact(value):
    # JavaScript clients lose precision above 2^53, so big integers travel as strings.
    return str(value) if isinstance(value, int) and abs(value) > 2**53 else value


class ClickHouse:
    """ClickHouse over HTTP(S); give its user read-only grants (readonly=1 or 2) as the second lock."""

    def __init__(self, cfg: ClickHouseConfig, masked_fields: frozenset[str] = frozenset()):
        self.cfg = cfg
        self.masked_fields = masked_fields
        self._table_functions: frozenset[str] | None = None
        self._log_comment: bool | None = None
        self.allowed_functions = INTERNAL_TABLE_FUNCTIONS | {f.lower() for f in cfg.allowed_table_functions}
        self.url = cfg.url.rstrip("/") + "/"
        self.session = requests.Session()
        self.session.headers.update({"X-ClickHouse-User": cfg.user, "X-ClickHouse-Key": cfg.password})
        if not cfg.verify_tls:
            self.session.verify = False
        elif cfg.ca_bundle:
            self.session.verify = cfg.ca_bundle

    def _scalar(self, sql: str) -> str | None:
        response = self.session.post(
            self.url, params={"default_format": "TabSeparatedRaw"}, data=sql.encode(), timeout=self.cfg.timeout_seconds
        )
        return response.text if response.ok else None

    def table_functions(self) -> frozenset[str]:
        if self._table_functions is None:
            names = self._scalar("SELECT lower(name) FROM system.table_functions")
            self._table_functions = frozenset(names.split()) | KNOWN_TABLE_FUNCTIONS if names else KNOWN_TABLE_FUNCTIONS
        return self._table_functions

    @property
    def log_comment(self) -> bool:
        # A readonly=1 user can't set any setting, log_comment included.
        if self._log_comment is None:
            self._log_comment = (self._scalar("SELECT getSetting('readonly')") or "").strip() != "1"
        return self._log_comment

    def check(self, sql: str, params: dict | None = None) -> str:
        """The statement without a trailing semicolon, once ClickHouse's parser shows it is a plain read."""
        statement = sql.strip().rstrip(";").strip()
        if not statement:
            raise ValueError("empty query")
        # The server's own parser sees every comment, quoting and string form the way execution will.
        response = self.session.post(
            self.url,
            params={"default_format": "TabSeparatedRaw", **(params or {})},
            data=f"EXPLAIN AST {statement}".encode(),
            timeout=self.cfg.timeout_seconds,
        )
        if not response.ok:
            raise ValueError(f"ClickHouse could not parse the query: {response.text[:1000]}")
        check_ast(response.text, self.allowed_functions, self.table_functions())
        return statement

    def query(
        self,
        sql: str,
        user: str,
        database: str | None = None,
        params: dict | None = None,
        max_rows: int | None = None,
        max_chars: int = 400_000,
        mask_columns: bool = True,
    ) -> dict:
        """Rows as lists, streamed and cut at max_rows or max_chars, so a huge result never reaches memory."""
        max_rows = max(1, min(max_rows or self.cfg.max_rows, self.cfg.max_rows))
        query_params = {f"param_{name}": param_value(value) for name, value in (params or {}).items()}
        statement = self.check(sql, query_params)
        query_id = str(uuid.uuid4())
        url_params = {
            **{k: str(v) for k, v in self.cfg.settings.items()},
            "default_format": "JSONCompactEachRowWithNamesAndTypes",
            "query_id": query_id,
        }
        if self.log_comment:
            url_params["log_comment"] = f"dataplane-mcp user={user}"
        if database or self.cfg.database:
            url_params["database"] = database or self.cfg.database
        url_params.update(query_params)
        return self._run(statement, url_params, query_id, max_rows, max_chars, mask_columns)

    def _run(self, statement, url_params, query_id, max_rows, max_chars, mask_columns) -> dict:
        response = self.session.post(
            self.url, params=url_params, data=statement.encode(), timeout=self.cfg.timeout_seconds, stream=True
        )
        with response:
            if not response.ok:
                raise RuntimeError(response.text[:2000])
            lines = response.iter_lines(decode_unicode=True)
            columns = json.loads(next(lines, "[]") or "[]")
            types = json.loads(next(lines, "[]") or "[]")
            # Convenience only: an alias gets around it. Column grants are the real control.
            masked = {i for i, name in enumerate(columns) if mask_columns and name.lower() in self.masked_fields}
            rows, chars, truncated = [], 0, None
            for line in lines:
                if not line:
                    continue
                if not line.startswith("["):
                    # An error after streaming started arrives as plain text in the body.
                    raise RuntimeError(line[:2000])
                if len(rows) >= max_rows:
                    truncated = f"more than {max_rows} rows"
                    break
                chars += len(line)
                if chars > max_chars:
                    truncated = f"more than {max_chars} characters"
                    break
                values = json.loads(line)
                rows.append([MASK if i in masked else _exact(mask(v, frozenset())) for i, v in enumerate(values)])
            summary = response.headers.get("X-ClickHouse-Summary")
        result = {
            "columns": [{"name": n, "type": t} for n, t in zip(columns, types, strict=False)],
            "rows": rows,
            "row_count": len(rows),
            "truncated": truncated,
            "query_id": query_id,
        }
        if truncated:
            result["hint"] = "aggregate, filter or add LIMIT; truncated results are not a complete answer"
        if summary:
            stats = json.loads(summary)
            result["stats"] = {k: stats[k] for k in ("read_rows", "read_bytes", "elapsed_ns") if k in stats}
        return result

    def rows(self, sql: str, user: str, params: dict | None = None) -> list[dict]:
        """Every row of an internal metadata query as dicts; fails rather than return part of them."""
        url_params = {
            "default_format": "JSONCompactEachRowWithNamesAndTypes",
            "query_id": str(uuid.uuid4()),
            **{f"param_{k}": param_value(v) for k, v in (params or {}).items()},
        }
        if self.log_comment:
            url_params["log_comment"] = f"dataplane-mcp user={user}"
        result = self._run(sql, url_params, url_params["query_id"], INTERNAL_ROWS, 50_000_000, mask_columns=False)
        if result["truncated"]:
            raise RuntimeError(f"metadata query returned {result['truncated']}; narrow it with a filter")
        names = [c["name"] for c in result["columns"]]
        return [dict(zip(names, r, strict=False)) for r in result["rows"]]
