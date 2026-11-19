# Python SQLite Change Data Capture (CDC) and JSONL Changefeed

Capture SQLite row changes transactionally and apply them to peer databases with resumable cursors. This Python library uses SQLite triggers to store typed after-images and old primary keys; a row update and its feed event commit or roll back together. JSON Lines events support caller-managed transport for local-first applications.

## Install from source

Requires Python 3.10 or newer and SQLite 3.24+ with JSON functions enabled:

```sh
git clone https://github.com/buxerr/sqlite-changefeed.git
cd sqlite-changefeed
python -m pip install .
```

## Capture and apply changes

Register the same persistent table schema on source and peer databases. The example below moves events directly; in an application, send the serialized records over your own transport.

```python
import sqlite3
from sqlite_changefeed import ChangeEvent, Changefeed

source_db = sqlite3.connect("source.db")
peer_db = sqlite3.connect("peer.db")
for db in (source_db, peer_db):
    db.execute("CREATE TABLE IF NOT EXISTS notes (id INTEGER PRIMARY KEY, text TEXT NOT NULL)")
    db.commit()

source = Changefeed(source_db)
peer = Changefeed(peer_db)
source.register_table("notes")
peer.register_table("notes")

source_db.execute("INSERT INTO notes (id, text) VALUES (?, ?)", (1, "hello"))
source_db.commit()

batch = source.read_events(after_seq=0, limit=100)
wire_records = [event.to_json() for event in batch]
peer.apply_events([ChangeEvent.from_json(record) for record in wire_records])
```

Store each source feed ID and last sequence for your transport cursor, then resume with `read_events(after_seq=cursor)`. The receiver persists `last_applied(source_feed_id)` atomically with row application, so re-delivery is safe. Once every peer acknowledges a sequence, call `prune_events(through_seq)` to reclaim history.

## Schema and replication rules

Register persistent tables with explicit primary keys and at most 100 columns. Peers must use matching registered columns and primary-key definitions. After a table migration, call `refresh_table(table, discard_history=True)` only after peers have synchronized or been reset; it discards old local events and rewrites the trigger payload shape.

If a database file is cloned to create another device, call `rotate_identity()` on the clone before it emits changes. This assigns a distinct source ID and clears copied local event history. Trigger events capture row images; they do not preserve original SQL statements, rowid values, or foreign-key ordering policy.

## Scope and limitations

The transport is caller-supplied. The library does not open sockets, encrypt events, delete old feed rows automatically, or choose a conflict policy. A target that rejects any row rolls back the complete batch and leaves its cursor unchanged. This is a row changefeed for cooperating SQLite databases, not a general-purpose multi-master conflict-resolution engine.

Run the Python unit suite with `PYTHONPATH=src python -m unittest discover -s tests -v`.

## License

MIT. See [LICENSE](LICENSE).
