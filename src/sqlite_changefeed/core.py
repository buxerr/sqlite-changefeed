"""Install transactional triggers and read/apply portable SQLite row changes."""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import math
import re
import sqlite3
from typing import Any, Iterable, Mapping
from uuid import uuid4


class ChangefeedError(ValueError):
    """Raised when a database schema or change batch is unsupported."""


_INTERNAL_PREFIX = "__sqlite_changefeed_"
_NAME = re.compile(r"^[^\x00]+$")
_MAX_BATCH = 10_000


def _quote(identifier: str) -> str:
    if not isinstance(identifier, str) or not _NAME.fullmatch(identifier):
        raise ChangefeedError("SQLite identifiers must be non-empty strings without NUL")
    return '"' + identifier.replace('"', '""') + '"'


def _sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _wire_value(value: Any) -> Any:
    if isinstance(value, bytes):
        return {"$sqlite_blob_hex": value.hex()}
    if value is None or isinstance(value, (str, int, float)):
        return value
    raise ChangefeedError(f"unsupported SQLite value type: {type(value).__name__}")


def _unwire_value(value: Any) -> Any:
    if isinstance(value, dict) and set(value) == {"$sqlite_blob_hex"}:
        encoded = value["$sqlite_blob_hex"]
        if not isinstance(encoded, str) or len(encoded) % 2 or not re.fullmatch(r"[0-9a-fA-F]*", encoded):
            raise ChangefeedError("invalid blob encoding in event")
        return bytes.fromhex(encoded)
    if value is None or isinstance(value, (str, int)) and not isinstance(value, bool):
        return value
    if isinstance(value, float) and math.isfinite(value):
        return value
    if isinstance(value, bool):
        return value
    raise ChangefeedError("event values must be SQLite scalar values")


@dataclass(frozen=True)
class ChangeEvent:
    """One committed row change with typed, transport-safe before-key and after-row values."""

    feed_id: str
    seq: int
    table: str
    operation: str
    key: Mapping[str, Any]
    row: Mapping[str, Any] | None

    def to_dict(self) -> dict[str, Any]:
        return {
            "feedId": self.feed_id,
            "seq": self.seq,
            "table": self.table,
            "operation": self.operation,
            "key": {name: _wire_value(value) for name, value in self.key.items()},
            "row": None if self.row is None else {name: _wire_value(value) for name, value in self.row.items()},
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "ChangeEvent":
        if not isinstance(value, Mapping):
            raise ChangefeedError("event must be an object")
        feed_id, seq = value.get("feedId"), value.get("seq")
        table, operation, key, row = value.get("table"), value.get("operation"), value.get("key"), value.get("row")
        if not isinstance(feed_id, str) or not feed_id or len(feed_id) > 64:
            raise ChangefeedError("event feedId must be a bounded non-empty string")
        if isinstance(seq, bool) or not isinstance(seq, int) or seq < 1:
            raise ChangefeedError("event seq must be a positive integer")
        if not isinstance(table, str) or not table or len(table) > 255:
            raise ChangefeedError("event table must be a bounded non-empty string")
        if not isinstance(operation, str) or operation not in {"I", "U", "D"} or not isinstance(key, Mapping):
            raise ChangefeedError("event operation or key is invalid")
        if operation == "D" and row is not None or operation != "D" and not isinstance(row, Mapping):
            raise ChangefeedError("delete events require no row; insert/update events require a row")
        if any(not isinstance(name, str) for name in key) or (row is not None and any(not isinstance(name, str) for name in row)):
            raise ChangefeedError("event row and key names must be strings")
        decoded_key = {name: _unwire_value(item) for name, item in key.items()}
        decoded_row = None if row is None else {name: _unwire_value(item) for name, item in row.items()}
        return cls(feed_id, seq, table, operation, decoded_key, decoded_row)

    @classmethod
    def from_json(cls, value: str) -> "ChangeEvent":
        try:
            parsed = json.loads(value)
        except (TypeError, json.JSONDecodeError) as error:
            raise ChangefeedError("event must contain valid JSON") from error
        return cls.from_dict(parsed)


class Changefeed:
    """Capture registered table changes and apply ordered events to a peer database.

    The supplied connection is borrowed and is never closed by this object. SQLite JSON
    functions are used only inside triggers; Python performs event decoding and transport.
    """

    def __init__(self, connection: sqlite3.Connection, *, feed_id: str | None = None):
        if not isinstance(connection, sqlite3.Connection):
            raise TypeError("connection must be a sqlite3.Connection")
        if feed_id is not None and (not isinstance(feed_id, str) or not feed_id or len(feed_id) > 64):
            raise ValueError("feed_id must be a bounded non-empty string")
        self.connection = connection
        self._savepoint = 0
        self._create_internal_tables()
        self.feed_id = self._ensure_feed_id(feed_id)
        self._registrations = self._load_registrations()

    def _next_savepoint(self) -> str:
        self._savepoint += 1
        return f"sqlite_changefeed_{self._savepoint}"

    def _create_internal_tables(self) -> None:
        try:
            self.connection.execute("SELECT json_object('probe', 1)").fetchone()
        except sqlite3.DatabaseError as error:
            raise ChangefeedError("SQLite JSON functions are required (json_object/json_array)") from error
        statements = (
            f"CREATE TABLE IF NOT EXISTS {_INTERNAL_PREFIX}meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)",
            f"CREATE TABLE IF NOT EXISTS {_INTERNAL_PREFIX}tables (table_name TEXT PRIMARY KEY, columns_json TEXT NOT NULL, pk_json TEXT NOT NULL)",
            f"CREATE TABLE IF NOT EXISTS {_INTERNAL_PREFIX}events (seq INTEGER PRIMARY KEY AUTOINCREMENT, feed_id TEXT NOT NULL, table_name TEXT NOT NULL, operation TEXT NOT NULL CHECK(operation IN ('I','U','D')), key_json TEXT NOT NULL, row_json TEXT)",
            f"CREATE INDEX IF NOT EXISTS {_INTERNAL_PREFIX}events_feed_seq ON {_INTERNAL_PREFIX}events(feed_id, seq)",
            f"CREATE TABLE IF NOT EXISTS {_INTERNAL_PREFIX}cursors (feed_id TEXT PRIMARY KEY, last_seq INTEGER NOT NULL CHECK(last_seq >= 0))",
        )
        for statement in statements:
            self.connection.execute(statement)
        self.connection.execute(
            f"INSERT OR IGNORE INTO {_INTERNAL_PREFIX}meta(key, value) VALUES ('suppress', '0')"
        )

    def _ensure_feed_id(self, requested: str | None) -> str:
        existing = self.connection.execute(
            f"SELECT value FROM {_INTERNAL_PREFIX}meta WHERE key = 'feed_id'"
        ).fetchone()
        if existing is not None:
            if requested is not None and requested != existing[0]:
                raise ChangefeedError("feed_id is already persisted; call rotate_identity() for a cloned database")
            return existing[0]
        chosen = requested if requested is not None else str(uuid4())
        self.connection.execute(
            f"INSERT INTO {_INTERNAL_PREFIX}meta(key, value) VALUES ('feed_id', ?)", (chosen,)
        )
        return chosen

    def rotate_identity(self, feed_id: str | None = None) -> str:
        """Assign a new source identity to a cloned database and discard its copied local events."""
        new_id = feed_id if feed_id is not None else str(uuid4())
        if not isinstance(new_id, str) or not new_id or len(new_id) > 64:
            raise ValueError("feed_id must be a bounded non-empty string")
        if new_id == self.feed_id:
            raise ValueError("new feed_id must differ from the current identity")
        savepoint = self._next_savepoint()
        self.connection.execute(f"SAVEPOINT {savepoint}")
        try:
            self.connection.execute(f"DELETE FROM {_INTERNAL_PREFIX}events WHERE feed_id = ?", (self.feed_id,))
            self.connection.execute(
                f"UPDATE {_INTERNAL_PREFIX}meta SET value = ? WHERE key = 'feed_id'", (new_id,)
            )
            self.connection.execute(f"RELEASE {savepoint}")
        except Exception:
            self.connection.execute(f"ROLLBACK TO {savepoint}")
            self.connection.execute(f"RELEASE {savepoint}")
            raise
        self.feed_id = new_id
        return new_id

    def _load_registrations(self) -> dict[str, tuple[tuple[str, ...], tuple[str, ...]]]:
        registrations: dict[str, tuple[tuple[str, ...], tuple[str, ...]]] = {}
        for table_name, columns_json, pk_json in self.connection.execute(
            f"SELECT table_name, columns_json, pk_json FROM {_INTERNAL_PREFIX}tables"
        ):
            registrations[table_name] = (tuple(json.loads(columns_json)), tuple(json.loads(pk_json)))
        return registrations

    def _table_shape(self, table: str) -> tuple[tuple[str, ...], tuple[str, ...]]:
        quoted = _quote(table)
        kind = self.connection.execute("SELECT type FROM sqlite_master WHERE name = ?", (table,)).fetchone()
        if kind is None or kind[0] != "table":
            raise ChangefeedError(f"registered object is not a persistent SQLite table: {table}")
        if table.startswith(_INTERNAL_PREFIX):
            raise ChangefeedError("internal changefeed tables cannot be registered")
        columns_info = self.connection.execute(f"PRAGMA table_info({quoted})").fetchall()
        if not columns_info:
            raise ChangefeedError(f"table has no columns: {table}")
        columns = tuple(row[1] for row in columns_info)
        if len(columns) > 100:
            raise ChangefeedError("registered tables are limited to 100 columns for broad SQLite JSON-function compatibility")
        primary = tuple(row[1] for row in sorted((row for row in columns_info if row[5]), key=lambda row: row[5]))
        if not primary:
            raise ChangefeedError(f"table must have an explicit primary key: {table}")
        return columns, primary

    def register_table(self, table: str) -> None:
        """Install insert/update/delete triggers for one table; idempotent for unchanged schemas."""
        columns, primary = self._table_shape(table)
        prior = self._registrations.get(table)
        if prior is not None:
            if prior != (columns, primary):
                raise ChangefeedError(f"registered table schema changed; recreate the feed after migrating {table}")
            trigger_hash = hashlib.sha256(table.encode("utf-8")).hexdigest()[:20]
            expected = {f"{_INTERNAL_PREFIX}{trigger_hash}_{suffix}" for suffix in ("ai", "au", "ad")}
            existing = {row[0] for row in self.connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'trigger' AND name IN (?, ?, ?)",
                tuple(expected),
            )}
            if existing == expected:
                return
        trigger_hash = hashlib.sha256(table.encode("utf-8")).hexdigest()[:20]
        quoted_table = _quote(table)
        old_values = [f"OLD.{_quote(column)}" for column in columns]
        new_values = [f"NEW.{_quote(column)}" for column in columns]
        old_keys = [f"OLD.{_quote(column)}" for column in primary]
        new_keys = [f"NEW.{_quote(column)}" for column in primary]
        trigger_specs = (
            ("ai", "INSERT", new_keys, new_values, "I"),
            ("au", "UPDATE", old_keys, new_values, "U"),
            ("ad", "DELETE", old_keys, None, "D"),
        )
        table_literal = _sql_string(table)
        guard = f"(SELECT value FROM {_INTERNAL_PREFIX}meta WHERE key = 'suppress') = '0'"
        savepoint = self._next_savepoint()
        self.connection.execute(f"SAVEPOINT {savepoint}")
        try:
            for suffix, action, key_values, row_values, operation in trigger_specs:
                name = _quote(f"{_INTERNAL_PREFIX}{trigger_hash}_{suffix}")
                key_json = self._json_sql_array(key_values)
                row_json = "NULL" if row_values is None else self._json_sql_array(row_values)
                self.connection.execute(
                    f"CREATE TRIGGER IF NOT EXISTS {name} AFTER {action} ON {quoted_table} WHEN {guard} BEGIN "
                    f"INSERT INTO {_INTERNAL_PREFIX}events(feed_id, table_name, operation, key_json, row_json) "
                    f"VALUES ((SELECT value FROM {_INTERNAL_PREFIX}meta WHERE key = 'feed_id'), "
                    f"{table_literal}, '{operation}', {key_json}, {row_json}); END"
                )
            if prior is None:
                self.connection.execute(
                    f"INSERT INTO {_INTERNAL_PREFIX}tables(table_name, columns_json, pk_json) VALUES (?, ?, ?)",
                    (table, json.dumps(columns), json.dumps(primary)),
                )
            self.connection.execute(f"RELEASE {savepoint}")
        except Exception:
            self.connection.execute(f"ROLLBACK TO {savepoint}")
            self.connection.execute(f"RELEASE {savepoint}")
            raise
        self._registrations[table] = (columns, primary)

    def refresh_table(self, table: str, *, discard_history: bool = False) -> None:
        """Reinstall registration after a schema migration, optionally discarding old row events.

        Coordinate this operation with every peer. Clearing the local history can make a peer
        miss changes unless it has acknowledged the old feed or is reset from a snapshot.
        """
        shape = self._table_shape(table)
        prior = self._registrations.get(table)
        if prior is None or prior == shape:
            self.register_table(table)
            return
        if not discard_history:
            raise ChangefeedError(
                f"registered table schema changed; call refresh_table({table!r}, discard_history=True) "
                "only after peers have synchronized or been reset"
            )
        trigger_hash = hashlib.sha256(table.encode("utf-8")).hexdigest()[:20]
        savepoint = self._next_savepoint()
        self.connection.execute(f"SAVEPOINT {savepoint}")
        try:
            for suffix in ("ai", "au", "ad"):
                trigger = _quote(f"{_INTERNAL_PREFIX}{trigger_hash}_{suffix}")
                self.connection.execute(f"DROP TRIGGER IF EXISTS {trigger}")
            self.connection.execute(f"DELETE FROM {_INTERNAL_PREFIX}events WHERE table_name = ?", (table,))
            self.connection.execute(
                f"UPDATE {_INTERNAL_PREFIX}tables SET columns_json = ?, pk_json = ? WHERE table_name = ?",
                (json.dumps(shape[0]), json.dumps(shape[1]), table),
            )
            self.connection.execute(f"RELEASE {savepoint}")
        except Exception:
            self.connection.execute(f"ROLLBACK TO {savepoint}")
            self.connection.execute(f"RELEASE {savepoint}")
            raise
        self._registrations[table] = shape
        self.register_table(table)

    @staticmethod
    def _json_sql_array(expressions: Iterable[str]) -> str:
        tagged = []
        for expression in expressions:
            tagged.append(
                "json_array(typeof({value}), CASE WHEN typeof({value}) = 'blob' "
                "THEN hex({value}) ELSE {value} END)".format(value=expression)
            )
        return "json_array(" + ", ".join(tagged) + ")"

    def read_events(self, after_seq: int = 0, limit: int = 500) -> tuple[ChangeEvent, ...]:
        """Read an ordered page after a source sequence cursor."""
        if isinstance(after_seq, bool) or not isinstance(after_seq, int) or after_seq < 0:
            raise ValueError("after_seq must be a non-negative integer")
        if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= _MAX_BATCH:
            raise ValueError(f"limit must be between 1 and {_MAX_BATCH}")
        rows = self.connection.execute(
            f"SELECT seq, feed_id, table_name, operation, key_json, row_json FROM {_INTERNAL_PREFIX}events "
            "WHERE seq > ? ORDER BY seq LIMIT ?",
            (after_seq, limit),
        ).fetchall()
        events = []
        for seq, feed_id, table, operation, key_json, row_json in rows:
            shape = self._registrations.get(table)
            if shape is None:
                raise ChangefeedError(f"event refers to an unregistered table: {table}")
            columns, primary = shape
            keys = self._decode_payload(key_json, primary)
            row = None if row_json is None else self._decode_payload(row_json, columns)
            events.append(ChangeEvent(feed_id, seq, table, operation, keys, row))
        return tuple(events)

    @staticmethod
    def _decode_payload(payload: str, names: tuple[str, ...]) -> dict[str, Any]:
        try:
            values = json.loads(payload)
        except (TypeError, json.JSONDecodeError) as error:
            raise ChangefeedError("stored changefeed payload is invalid JSON") from error
        if not isinstance(values, list) or len(values) != len(names):
            raise ChangefeedError("stored changefeed payload does not match the registered schema")
        decoded = {}
        for name, tagged in zip(names, values):
            if not isinstance(tagged, list) or len(tagged) != 2:
                raise ChangefeedError("stored changefeed value has an invalid type tag")
            kind, value = tagged
            if kind == "blob":
                if not isinstance(value, str) or len(value) % 2 or not re.fullmatch(r"[0-9a-fA-F]*", value):
                    raise ChangefeedError("stored blob is not valid hexadecimal")
                decoded[name] = bytes.fromhex(value)
            elif kind == "null" and value is None:
                decoded[name] = None
            elif kind == "text" and isinstance(value, str):
                decoded[name] = value
            elif kind == "integer" and isinstance(value, int) and not isinstance(value, bool):
                decoded[name] = value
            elif kind == "real" and isinstance(value, (int, float)) and not isinstance(value, bool):
                decoded[name] = float(value)
            else:
                raise ChangefeedError(f"stored value does not match SQLite type tag {kind!r}")
        return decoded

    def last_sequence(self) -> int:
        return self.connection.execute(f"SELECT COALESCE(MAX(seq), 0) FROM {_INTERNAL_PREFIX}events").fetchone()[0]

    def prune_events(self, through_seq: int) -> int:
        """Delete local feed history through a sequence after all peers have advanced past it."""
        if isinstance(through_seq, bool) or not isinstance(through_seq, int) or through_seq < 0:
            raise ValueError("through_seq must be a non-negative integer")
        cursor = self.connection.execute(
            f"DELETE FROM {_INTERNAL_PREFIX}events WHERE feed_id = ? AND seq <= ?",
            (self.feed_id, through_seq),
        )
        return cursor.rowcount

    def last_applied(self, feed_id: str) -> int:
        row = self.connection.execute(
            f"SELECT last_seq FROM {_INTERNAL_PREFIX}cursors WHERE feed_id = ?", (feed_id,)
        ).fetchone()
        return 0 if row is None else row[0]

    def apply_events(self, events: Iterable[ChangeEvent | Mapping[str, Any]]) -> int:
        """Apply one source batch atomically, suppress echo triggers, and persist its cursor."""
        batch_items = []
        for event in events:
            candidate = event.to_dict() if isinstance(event, ChangeEvent) else event
            batch_items.append(ChangeEvent.from_dict(candidate))
            if len(batch_items) > _MAX_BATCH:
                raise ChangefeedError(f"batch cannot exceed {_MAX_BATCH} events")
        batch = tuple(batch_items)
        if not batch:
            return 0
        feed_ids = {event.feed_id for event in batch}
        if len(feed_ids) != 1:
            raise ChangefeedError("one apply batch must contain events from exactly one source feed")
        feed_id = batch[0].feed_id
        if any(current.seq <= previous.seq for previous, current in zip(batch, batch[1:])):
            raise ChangefeedError("events must be ordered by strictly increasing sequence")
        savepoint = self._next_savepoint()
        self.connection.execute(f"SAVEPOINT {savepoint}")
        try:
            prior_cursor = self.last_applied(feed_id)
            pending = tuple(event for event in batch if event.seq > prior_cursor)
            if pending:
                self.connection.execute(
                    f"INSERT INTO {_INTERNAL_PREFIX}meta(key, value) VALUES ('suppress', '1') "
                    "ON CONFLICT(key) DO UPDATE SET value = '1'"
                )
                for event in pending:
                    self._apply_one(event)
                self.connection.execute(
                    f"INSERT INTO {_INTERNAL_PREFIX}cursors(feed_id, last_seq) VALUES (?, ?) "
                    "ON CONFLICT(feed_id) DO UPDATE SET last_seq = excluded.last_seq",
                    (feed_id, pending[-1].seq),
                )
                self.connection.execute(
                    f"UPDATE {_INTERNAL_PREFIX}meta SET value = '0' WHERE key = 'suppress'"
                )
            self.connection.execute(f"RELEASE {savepoint}")
        except Exception:
            self.connection.execute(f"ROLLBACK TO {savepoint}")
            self.connection.execute(f"RELEASE {savepoint}")
            raise
        return len(pending)

    def _apply_one(self, event: ChangeEvent) -> None:
        shape = self._registrations.get(event.table)
        if shape is None:
            raise ChangefeedError(f"target table is not registered: {event.table}")
        columns, primary = shape
        if set(event.key) != set(primary):
            raise ChangefeedError(f"event key fields do not match the registered primary key for {event.table}")
        table = _quote(event.table)
        key_where = " AND ".join(f"{_quote(name)} IS ?" for name in primary)
        key_values = tuple(event.key[name] for name in primary)
        if event.operation == "D":
            if event.row is not None:
                raise ChangefeedError("delete event must not include a row")
            self.connection.execute(f"DELETE FROM {table} WHERE {key_where}", key_values)
            return
        if event.operation not in {"I", "U"} or event.row is None or set(event.row) != set(columns):
            raise ChangefeedError(f"event row fields do not match the registered schema for {event.table}")
        row_values = tuple(event.row[column] for column in columns)
        new_key_values = tuple(event.row[column] for column in primary)
        same_key = all(left == right for left, right in zip(key_values, new_key_values))
        if event.operation == "U" and not same_key:
            self.connection.execute(f"DELETE FROM {table} WHERE {key_where}", key_values)
        placeholders = ", ".join("?" for _ in columns)
        names = ", ".join(_quote(name) for name in columns)
        conflict = ", ".join(_quote(name) for name in primary)
        updates = ", ".join(f"{_quote(name)} = excluded.{_quote(name)}" for name in columns if name not in primary)
        if updates:
            sql = f"INSERT INTO {table} ({names}) VALUES ({placeholders}) ON CONFLICT ({conflict}) DO UPDATE SET {updates}"
        else:
            sql = f"INSERT INTO {table} ({names}) VALUES ({placeholders}) ON CONFLICT ({conflict}) DO NOTHING"
        self.connection.execute(sql, row_values)
