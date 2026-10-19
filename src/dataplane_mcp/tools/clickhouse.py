from ..clickhouse import ClickHouse
from ..config import ClickHouseConfig
from ..paging import page
from ..server import Toolbox, env_type

SYSTEM_DATABASES = ["system", "INFORMATION_SCHEMA", "information_schema"]

HEALTH_CHECKS = {
    "replicas_with_problems": """
        SELECT database, table, is_readonly, is_session_expired, queue_size, inserts_in_queue,
               merges_in_queue, absolute_delay, last_queue_update_exception
        FROM system.replicas
        WHERE is_readonly OR is_session_expired OR queue_size > 20 OR absolute_delay > 300
              OR last_queue_update_exception != ''
        ORDER BY absolute_delay DESC, queue_size DESC LIMIT {limit:UInt32}""",
    "disks": """
        SELECT name, path, formatReadableSize(free_space) AS free, formatReadableSize(total_space) AS total,
               round(100 * (1 - free_space / total_space), 1) AS used_percent
        FROM system.disks ORDER BY used_percent DESC""",
    "partitions_with_most_active_parts": """
        SELECT database, table, partition, count() AS active_parts
        FROM system.parts WHERE active
        GROUP BY database, table, partition ORDER BY active_parts DESC LIMIT {limit:UInt32}""",
    "longest_running_queries": """
        SELECT query_id, user, round(elapsed, 1) AS elapsed_s, formatReadableSize(memory_usage) AS memory,
               substring(query, 1, 300) AS query
        FROM system.processes ORDER BY elapsed DESC LIMIT {limit:UInt32}""",
    "merges": """
        SELECT count() AS running, max(round(elapsed)) AS longest_s, sum(total_size_bytes_compressed) AS bytes
        FROM system.merges""",
    "stuck_mutations": """
        SELECT database, table, mutation_id, create_time, parts_to_do, latest_fail_reason
        FROM system.mutations WHERE NOT is_done ORDER BY create_time LIMIT {limit:UInt32}""",
    "recent_errors": """
        SELECT name, code, value AS count, last_error_time, substring(last_error_message, 1, 300) AS message
        FROM system.errors WHERE last_error_time > now() - INTERVAL 1 HOUR
        ORDER BY last_error_time DESC LIMIT {limit:UInt32}""",
}


def register(box: Toolbox, configs: dict[str, ClickHouseConfig]) -> None:
    clickhouse = {name: ClickHouse(cfg, box.cfg.masked_fields) for name, cfg in configs.items()}
    max_chars = box.cfg.max_result_chars
    Env = env_type(list(configs))

    @box.tool
    def clickhouse_query(
        user: str,
        env: Env,
        sql: str,
        database: str | None = None,
        params: dict[str, str | int | float | bool | list[str | int | float | bool | None] | None] | None = None,
        max_rows: int | None = None,
    ) -> dict:
        """Run one read-only ClickHouse statement (SELECT, WITH, SHOW, DESCRIBE, EXISTS, EXPLAIN).

        Rows come back as lists in column order. The result stops at max_rows (capped by the server
        setting) or the size cap and then says truncated; a truncated result is not a complete answer.
        Put values in params and reference them as {name:Type}, e.g. WHERE id = {id:UInt64}.
        Only table functions that read ClickHouse itself (numbers, merge, clusterAllReplicas, ...) are allowed;
        url, s3, file, remote and the like are refused.
        Before heavy queries run EXPLAIN ESTIMATE <query> to see rows and marks to read.
        """
        return clickhouse[env].query(sql, user, database, params, max_rows, max_chars)

    @box.tool
    def clickhouse_databases(user: str, env: Env) -> dict:
        """Databases with their engine, table count and total size."""
        rows = clickhouse[env].rows(
            """SELECT d.name AS name, d.engine AS engine, count(t.name) AS tables,
                      formatReadableSize(sum(t.total_bytes)) AS size, d.comment AS comment
               FROM system.databases d LEFT JOIN system.tables t ON t.database = d.name
               GROUP BY d.name, d.engine, d.comment ORDER BY d.name""",
            user,
        )
        return {"databases": rows}

    @box.tool
    def clickhouse_tables(
        user: str,
        env: Env,
        database: str | None = None,
        contains: str | None = None,
        include_system: bool = False,
        offset: int = 0,
        limit: int = 100,
    ) -> dict:
        """Tables and views with engine, rows, size and comment, sorted by database and name.

        contains filters on the table name (case-insensitive). Works for Kafka, S3 and other engines
        because it only reads metadata.
        """
        rows = clickhouse[env].rows(
            """SELECT database, name, engine, total_rows AS rows, total_bytes AS bytes,
                      formatReadableSize(total_bytes) AS size, comment
               FROM system.tables
               WHERE ({database:Nullable(String)} IS NULL OR database = {database:Nullable(String)})
                 AND ({contains:Nullable(String)} IS NULL OR positionCaseInsensitive(name, {contains:Nullable(String)}) > 0)
                 AND ({include_system:Bool} OR database NOT IN {system:Array(String)})
                 AND NOT is_temporary
               ORDER BY database, name""",
            user,
            {
                "database": database,
                "contains": contains,
                "include_system": include_system,
                "system": SYSTEM_DATABASES,
            },
        )
        return page(rows, offset, limit)

    @box.tool
    def clickhouse_describe_table(user: str, env: Env, database: str, table: str) -> dict:
        """Columns (name, type, default, comment) plus engine, sorting, primary and partition keys and size of a table."""
        params = {"database": database, "table": table}
        info = clickhouse[env].rows(
            """SELECT engine_full, sorting_key, primary_key, partition_key, sampling_key, total_rows,
                      formatReadableSize(total_bytes) AS size, comment
               FROM system.tables WHERE database = {database:String} AND name = {table:String}""",
            user,
            params,
        )
        if not info:
            raise ValueError(f"no table {database}.{table}; use clickhouse_tables to find it")
        columns = clickhouse[env].rows(
            """SELECT name, type, default_kind, default_expression, comment, is_in_primary_key, is_in_sorting_key
               FROM system.columns WHERE database = {database:String} AND table = {table:String}
               ORDER BY position""",
            user,
            params,
        )
        return {"database": database, "table": table, **info[0], "columns": columns}

    @box.tool
    def clickhouse_search_columns(
        user: str, env: Env, contains: str, database: str | None = None, offset: int = 0, limit: int = 100
    ) -> dict:
        """Find columns whose name or comment contains a string (case-insensitive), across all user tables."""
        rows = clickhouse[env].rows(
            """SELECT database, table, name, type, comment
               FROM system.columns
               WHERE (positionCaseInsensitive(name, {contains:String}) > 0
                      OR positionCaseInsensitive(comment, {contains:String}) > 0)
                 AND ({database:Nullable(String)} IS NULL OR database = {database:Nullable(String)})
                 AND database NOT IN {system:Array(String)}
               ORDER BY database, table, name""",
            user,
            {
                "contains": contains,
                "database": database,
                "system": SYSTEM_DATABASES,
            },
        )
        return page(rows, offset, limit)

    @box.tool
    def clickhouse_health(user: str, env: Env, limit: int = 10) -> dict:
        """Server health in one call: version and uptime, replicas with problems, disk use, partitions with the
        most active parts, longest running queries, merges, unfinished mutations and errors in the last hour.

        Checks the user can't read come back as an error string instead of failing the whole call.
        """
        client = clickhouse[env]
        report = {
            "server": client.rows("SELECT version() AS version, uptime() AS uptime_s, hostName() AS host", user)[0]
        }
        for name, sql in HEALTH_CHECKS.items():
            try:
                report[name] = client.rows(sql, user, {"limit": max(1, limit)})
            except RuntimeError as e:
                report[name] = {"error": str(e)[:300]}
        return report
