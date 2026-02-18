"""Translates an OSI (Open Semantic Interchange) YAML file into a Snowflake Semantic View DDL statement."""

import re
from pathlib import Path
from typing import Any

import yaml


def _get_snowflake_expression(field: dict[str, Any]) -> str | None:
    """Return the Snowflake-dialect expression for a field, falling back to ANSI_SQL."""
    dialects: list[dict[str, str]] = field.get("expression", {}).get("dialects", [])
    for d in dialects:
        if d.get("dialect") == "Snowflake":
            return d["expression"]
    for d in dialects:
        if d.get("dialect") == "ANSI_SQL":
            return d["expression"]
    return None


def _synonyms_clause(obj: dict[str, Any]) -> str:
    """Return a WITH SYNONYMS clause if ai_context.synonyms is defined, else empty string."""
    synonyms: list[str] = obj.get("ai_context", {}).get("synonyms", [])
    if not synonyms:
        return ""
    quoted = ", ".join(f"'{s}'" for s in synonyms)
    return f" WITH SYNONYMS = ({quoted})"


def _comment_clause(obj: dict[str, Any]) -> str:
    description: str = obj.get("description", "").strip().replace("'", "''")
    if not description:
        return ""
    # Collapse multi-line descriptions
    description = " ".join(description.split())
    return f" COMMENT = '{description}'"


def osi_yaml_to_snowflake_semantic_view_ddl(
    osi_yaml_path: Path,
    target_database: str,
    target_schema: str,
    view_name: str | None = None,
) -> str:
    """Parse an OSI YAML file and return a CREATE OR REPLACE SEMANTIC VIEW DDL string.

    Args:
        osi_yaml_path: Path to the OSI YAML file.
        target_database: Snowflake database where the semantic view will be created.
        target_schema: Snowflake schema where the semantic view will be created.
        view_name: Override the semantic view name. Defaults to the model name in the YAML.

    Returns:
        A complete CREATE OR REPLACE SEMANTIC VIEW DDL string.
    """
    raw = yaml.safe_load(osi_yaml_path.read_text())

    models: list[dict[str, Any]] = raw.get("semantic_model", [])
    if not models:
        raise ValueError(f"No semantic_model entries found in {osi_yaml_path}")
    if len(models) > 1:
        raise ValueError(
            f"osi_yaml_to_snowflake_semantic_view_ddl only supports a single semantic_model per file; "
            f"found {len(models)} in {osi_yaml_path}"
        )

    model = models[0]
    sv_name = view_name or model["name"]
    model_description = model.get("description", "").strip().replace("'", "''")
    model_description = " ".join(model_description.split())

    datasets: list[dict[str, Any]] = model.get("datasets", [])
    if not datasets:
        raise ValueError(f"No datasets defined in semantic_model '{sv_name}'")

    # ── TABLES clause ─────────────────────────────────────────────────────────
    table_lines: list[str] = []
    for ds in datasets:
        alias = ds["name"]
        source: str = ds["source"]
        pks: list[str] = ds.get("primary_key", [])
        pk_clause = f" PRIMARY KEY ({', '.join(pks)})" if pks else ""
        table_lines.append(f"    {alias} AS {source}{pk_clause}")

    tables_clause = "TABLES (\n" + ",\n".join(table_lines) + "\n)"

    # ── Classify fields into FACTS (numeric) and DIMENSIONS (everything else) ──
    # OSI doesn't strictly separate facts from dimensions at the field level;
    # we use is_time and data role heuristics:
    #   - boolean flags → DIMENSIONS
    #   - time fields (is_time: true) → DIMENSIONS
    #   - numeric leaf columns used in metric expressions → FACTS
    #   - everything else → DIMENSIONS
    #
    # We collect metric expressions to identify which fields are raw numeric inputs.
    # Use regex word-boundary matching to avoid substring false positives
    # (e.g. field "ar" must not match expression "SUM(arr_by_month.arr)").
    metric_expressions: list[str] = [
        _get_snowflake_expression(m) or "" for m in model.get("metrics", [])
    ]

    def _field_referenced_in_metrics(alias: str, fname: str) -> bool:
        """Return True only if fname appears as a qualified column ref (alias.fname)
        or unqualified word-boundary match in any metric expression.
        """
        qualified = re.compile(rf"\b{re.escape(alias)}\.{re.escape(fname)}\b")
        unqualified = re.compile(rf"\b{re.escape(fname)}\b")
        return any(qualified.search(mex) or unqualified.search(mex) for mex in metric_expressions)

    fact_lines: list[str] = []
    dimension_lines: list[str] = []

    for ds in datasets:
        alias = ds["name"]
        primary_keys: set[str] = set(ds.get("primary_key", []))
        for field in ds.get("fields", []):
            fname = field["name"]
            expr = _get_snowflake_expression(field)
            if expr is None:
                continue

            synonyms = _synonyms_clause(field)
            comment = _comment_clause(field)
            is_time = field.get("dimension", {}).get("is_time", False)

            # A field is a fact if it is referenced in a metric expression (checked
            # via word-boundary regex, not substring), is not a time or boolean column,
            # and is not an identifier (primary key / foreign key → dimensions).
            is_identifier = fname.endswith("_id") or fname in primary_keys
            is_fact = (
                not is_time
                and not is_identifier
                and not fname.startswith("is_")
                and _field_referenced_in_metrics(alias, fname)
            )

            col_def = f"    {alias}.{fname} AS {expr}{synonyms}{comment}"
            if is_fact:
                fact_lines.append(col_def)
            else:
                dimension_lines.append(col_def)

    # ── METRICS clause ────────────────────────────────────────────────────────
    metric_lines: list[str] = []
    for m in model.get("metrics", []):
        mname = m["name"]
        expr = _get_snowflake_expression(m)
        if expr is None:
            continue
        synonyms = _synonyms_clause(m)
        comment = _comment_clause(m)
        # Metrics without a dataset prefix are derived metrics - keep as-is
        metric_lines.append(f"    {mname} AS {expr}{synonyms}{comment}")

    # ── Assemble DDL ──────────────────────────────────────────────────────────
    fully_qualified_name = f"{target_database}.{target_schema}.{sv_name}"
    lines: list[str] = [f"CREATE OR REPLACE SEMANTIC VIEW {fully_qualified_name}"]
    lines.append(tables_clause)

    if fact_lines:
        lines.append("FACTS (\n" + ",\n".join(fact_lines) + "\n)")

    if dimension_lines:
        lines.append("DIMENSIONS (\n" + ",\n".join(dimension_lines) + "\n)")

    if metric_lines:
        lines.append("METRICS (\n" + ",\n".join(metric_lines) + "\n)")

    if model_description:
        lines.append(f"COMMENT = '{model_description}'")

    return "\n".join(lines) + ";"
