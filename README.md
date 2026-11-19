# sqlite-changefeed

Trigger-based row change capture and resumable peer apply for SQLite. A row update and its feed event commit or roll back together. Events contain the old primary key and a typed after-image, including BLOB-safe transport encoding. Apply batches in order; a durable cursor makes re-delivery safe and prevents an old batch from overwriting newer peer state.

## Quick start

```python
import sqlite3
from sqlite_changefeed import Changefeed

db = sqlite3.connect("device.db")
feed = Changefeed(db)
feed.register_table("notes")

# Your normal INSERT/UPDATE/DELETE statements now create transactional feed rows.
events = feed.read_events(after_seq=0, limit=100)
json_lines = [event.to_json() for event in events]  # hand these to your transport

# On a peer database, after registering the same table schema:
peer = Changefeed(peer_db)
peer.register_table("notes")
peer.apply_events(received_events)
```

The transport is intentionally caller-supplied. `ChangeEvent.to_json()` and `ChangeEvent.from_json()` provide compact JSON Lines records; `received_events` above means decoded events from those records. Store the source feed ID and last sequence per sender and use `read_events(after_seq=cursor)`. The receiver exposes `last_applied(source_feed_id)` and persists its per-source cursor atomically with row application. Rows use their primary key as identity; application-level conflict policy and multi-master conflict resolution belong above this library. Once every peer acknowledges a sequence, `prune_events(through_seq)` can reclaim that history. If a database file is cloned to create a second device, call `rotate_identity()` on the clone before it emits changes; this assigns it a distinct source ID and clears copied local event history.

## Requirements and limits

Python 3.10+ and SQLite 3.24+ with JSON functions enabled. Register only persistent tables with explicit primary keys and at most 100 columns. Both peers must have matching registered columns and primary key definitions. After a table migration, call `refresh_table(table, discard_history=True)` only after peers have synchronized or been reset; this discards that table's old local events and rewrites its trigger payload shape. Triggers capture row images and do not preserve original SQL statements, rowid values, or foreign-key ordering policy. A target that rejects any row rolls back the complete batch and leaves its cursor unchanged.

The library does not open sockets, encrypt events, delete old feed rows automatically, or decide merge conflicts. Those policies stay with the embedding application.

## Development

```sh
PYTHONPATH=src python -m unittest discover -s tests -v
```

Runtime has no third-party dependencies. MIT licensed.
