"""Schema-level invariants for qiita.email_receipt and the work_ticket
notification columns, plus a StrEnum <-> CHECK parity guard.

The status CHECK is TEXT/CHECK (not a Postgres ENUM, per the carve-out), so it
is out of scope for ENUM_PAIRS. This file guards the same drift a light way:
it reads pg_get_constraintdef and asserts the values match
qiita_common.models.EmailReceiptStatus exactly. Pattern copied from
test_mask_definition_schema.py / test_sequence_range_schema.py.
"""

import pytest
from qiita_common.models import EmailReceiptStatus

pytestmark = pytest.mark.db


async def test_email_receipt_columns(postgres_pool):
    rows = await postgres_pool.fetch(
        "SELECT a.attname, format_type(a.atttypid, a.atttypmod) AS type,"
        "       a.attnotnull"
        "  FROM pg_attribute a"
        "  JOIN pg_class c ON c.oid = a.attrelid"
        "  JOIN pg_namespace n ON n.oid = c.relnamespace"
        " WHERE n.nspname = 'qiita'"
        "   AND c.relname = 'email_receipt'"
        "   AND a.attnum > 0"
        "   AND NOT a.attisdropped"
        " ORDER BY a.attnum"
    )
    cols = {r["attname"]: (r["type"], r["attnotnull"]) for r in rows}
    assert cols, "qiita.email_receipt is missing"
    assert cols["idx"] == ("bigint", True)
    assert cols["template_name"] == ("text", True)
    assert cols["template_context"] == ("jsonb", True)
    assert cols["recipient_email"][0] == "citext"
    assert cols["recipient_email"][1] is True
    assert cols["recipient_principal_idx"] == ("bigint", False)
    assert cols["subject"] == ("text", True)
    assert cols["body_text"] == ("text", True)
    assert cols["body_html"] == ("text", False)
    assert cols["status"] == ("text", True)
    assert cols["transport"] == ("text", True)
    assert cols["provider_message_id"] == ("text", False)
    assert cols["attempts"] == ("integer", True)
    assert cols["error"] == ("text", False)
    assert cols["template_sha"] == ("text", False)
    assert cols["created_at"][0].startswith("timestamp with time zone")
    assert cols["sent_at"][0].startswith("timestamp with time zone")
    assert cols["updated_at"][0].startswith("timestamp with time zone")


async def test_email_receipt_status_check_matches_strenum(postgres_pool):
    defs = await postgres_pool.fetch(
        "SELECT c.conname, pg_get_constraintdef(c.oid) AS def"
        "  FROM pg_constraint c"
        "  JOIN pg_class ct ON ct.oid = c.conrelid"
        "  JOIN pg_namespace cn ON cn.oid = ct.relnamespace"
        " WHERE c.contype = 'c'"
        "   AND cn.nspname = 'qiita' AND ct.relname = 'email_receipt'"
    )
    status_defs = [d["def"] for d in defs if "status" in d["def"]]
    assert len(status_defs) == 1, status_defs
    check_def = status_defs[0]
    # Every StrEnum value appears in the CHECK...
    for value in EmailReceiptStatus:
        assert f"'{value.value}'" in check_def, (value, check_def)
    # ...and the CHECK introduces no value the StrEnum lacks. The def looks like
    # CHECK (status = ANY (ARRAY['pending'::text, ...])). Extract the quoted
    # literals and compare sets.
    import re

    quoted = set(re.findall(r"'([a-z_]+)'", check_def))
    assert quoted == {v.value for v in EmailReceiptStatus}


async def test_email_receipt_gin_and_btree_indexes_exist(postgres_pool):
    rows = await postgres_pool.fetch(
        "SELECT i.relname AS index_name, am.amname AS method"
        "  FROM pg_index x"
        "  JOIN pg_class t ON t.oid = x.indrelid"
        "  JOIN pg_class i ON i.oid = x.indexrelid"
        "  JOIN pg_am am ON am.oid = i.relam"
        "  JOIN pg_namespace n ON n.oid = t.relnamespace"
        " WHERE n.nspname = 'qiita' AND t.relname = 'email_receipt'"
    )
    by_method = {r["index_name"]: r["method"] for r in rows}
    assert by_method.get("email_receipt_template_context_idx") == "gin", by_method
    assert by_method.get("email_receipt_recipient_idx") == "btree", by_method


async def test_email_receipt_updated_at_trigger_present(postgres_pool):
    trg = await postgres_pool.fetchval(
        "SELECT tgname FROM pg_trigger t"
        "  JOIN pg_class c ON c.oid = t.tgrelid"
        "  JOIN pg_namespace n ON n.oid = c.relnamespace"
        " WHERE n.nspname = 'qiita' AND c.relname = 'email_receipt'"
        "   AND NOT t.tgisinternal"
    )
    assert trg == "email_receipt_set_updated_at"


async def test_work_ticket_notify_columns(postgres_pool):
    rows = await postgres_pool.fetch(
        "SELECT a.attname, format_type(a.atttypid, a.atttypmod) AS type, a.attnotnull"
        "  FROM pg_attribute a"
        "  JOIN pg_class c ON c.oid = a.attrelid"
        "  JOIN pg_namespace n ON n.oid = c.relnamespace"
        " WHERE n.nspname = 'qiita' AND c.relname = 'work_ticket'"
        "   AND a.attname IN ('notified_at', 'notify_attempts')"
    )
    cols = {r["attname"]: (r["type"], r["attnotnull"]) for r in rows}
    assert cols["notified_at"][0].startswith("timestamp with time zone")
    assert cols["notified_at"][1] is False
    assert cols["notify_attempts"] == ("integer", True)


async def test_work_ticket_owed_partial_index_exists(postgres_pool):
    definition = await postgres_pool.fetchval(
        "SELECT pg_get_indexdef(i.oid)"
        "  FROM pg_class i"
        "  JOIN pg_namespace n ON n.oid = i.relnamespace"
        " WHERE n.nspname = 'qiita' AND i.relname = 'qiita_work_ticket_email_owed_idx'"
    )
    assert definition is not None, "qiita_work_ticket_email_owed_idx is missing"
    assert "notified_at IS NULL" in definition
    assert "retriable" in definition
