import json
import sqlite3
import unittest

from sqlite_changefeed import ChangeEvent, Changefeed, ChangefeedError


def open_database():
    connection = sqlite3.connect(":memory:")
    connection.execute("CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT NOT NULL, payload BLOB, rank REAL)")
    connection.execute("CREATE TABLE labels (note_id INTEGER NOT NULL, name TEXT NOT NULL, PRIMARY KEY(name, note_id))")
    return connection


class ChangefeedTests(unittest.TestCase):
    def setUp(self):
        self.source = open_database()
        self.target = open_database()
        self.source_feed = Changefeed(self.source)
        self.target_feed = Changefeed(self.target)
        for feed in (self.source_feed, self.target_feed):
            feed.register_table("notes")
            feed.register_table("labels")
        self.source.commit()
        self.target.commit()

    def tearDown(self):
        self.source.close()
        self.target.close()

    def test_capture_is_atomic_and_preserves_sqlite_scalar_types(self):
        self.source.execute("INSERT INTO notes VALUES (?, ?, ?, ?)", (1, "hello", b"\x00\xff", 1.5))
        self.source.commit()
        events = self.source_feed.read_events()
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].operation, "I")
        self.assertEqual(events[0].row, {"id": 1, "body": "hello", "payload": b"\x00\xff", "rank": 1.5})
        self.assertEqual(ChangeEvent.from_json(events[0].to_json()), events[0])

        self.source.execute("BEGIN")
        self.source.execute("UPDATE notes SET body = 'rolled back' WHERE id = 1")
        self.source.rollback()
        self.assertEqual(self.source_feed.last_sequence(), 1)
        self.assertEqual(self.source.execute("SELECT body FROM notes WHERE id = 1").fetchone()[0], "hello")

    def test_apply_insert_update_delete_and_resume_without_echo(self):
        self.source.execute("INSERT INTO notes VALUES (1, 'one', X'01', 1.0)")
        self.source.execute("UPDATE notes SET body = 'two' WHERE id = 1")
        self.source.execute("DELETE FROM notes WHERE id = 1")
        events = self.source_feed.read_events(limit=2)
        self.assertEqual(self.target_feed.apply_events(events), 2)
        self.assertEqual(self.target_feed.apply_events(self.source_feed.read_events(after_seq=events[-1].seq)), 1)
        self.assertEqual(self.target.execute("SELECT COUNT(*) FROM notes").fetchone()[0], 0)
        self.assertEqual(self.target_feed.last_sequence(), 0)
        self.assertEqual(self.target_feed.last_applied(self.source_feed.feed_id), 3)
        self.assertEqual(self.target_feed.apply_events(self.source_feed.read_events()), 0)

    def test_prunes_only_through_the_requested_acknowledged_sequence(self):
        self.source.executemany("INSERT INTO notes VALUES (?, ?, NULL, NULL)", [(1, "one"), (2, "two"), (3, "three")])
        events = self.source_feed.read_events()
        self.assertEqual(len(events), 3)
        self.assertEqual(self.source_feed.prune_events(events[1].seq), 2)
        self.assertEqual([event.seq for event in self.source_feed.read_events()], [events[2].seq])

    def test_apply_batch_rolls_back_rows_and_cursor_on_schema_failure(self):
        self.source.execute("INSERT INTO notes VALUES (1, 'one', NULL, 1)")
        event = self.source_feed.read_events()[0]
        self.target.execute("DROP TABLE notes")
        with self.assertRaises(sqlite3.OperationalError):
            self.target_feed.apply_events([event])
        self.assertEqual(self.target_feed.last_applied(self.source_feed.feed_id), 0)
        suppression = self.target.execute("SELECT value FROM __sqlite_changefeed_meta WHERE key='suppress'").fetchone()[0]
        self.assertEqual(suppression, "0")

    def test_primary_key_update_moves_the_row_and_composite_keys_work(self):
        self.source.execute("INSERT INTO notes VALUES (1, 'one', NULL, NULL)")
        self.source.execute("UPDATE notes SET id = 2, body = 'two' WHERE id = 1")
        self.target_feed.apply_events(self.source_feed.read_events())
        self.assertEqual(self.target.execute("SELECT id, body FROM notes").fetchall(), [(2, "two")])
        self.source.execute("INSERT INTO labels VALUES (2, 'blue')")
        wire_events = [ChangeEvent.from_json(event.to_json()) for event in self.source_feed.read_events(after_seq=2)]
        self.target_feed.apply_events(wire_events)
        self.assertEqual(self.target.execute("SELECT note_id, name FROM labels").fetchall(), [(2, "blue")])

    def test_schema_registration_is_idempotent_and_rejects_keyless_or_changed_tables(self):
        self.source_feed.register_table("notes")
        self.source.execute("CREATE TABLE keyless (value TEXT)")
        with self.assertRaisesRegex(ChangefeedError, "explicit primary key"):
            self.source_feed.register_table("keyless")
        self.source.execute("ALTER TABLE notes ADD COLUMN extra TEXT")
        with self.assertRaisesRegex(ChangefeedError, "schema changed"):
            self.source_feed.register_table("notes")

    def test_schema_refresh_requires_explicit_history_discard_and_rebuilds_triggers(self):
        self.source.execute("INSERT INTO notes VALUES (1, 'old', NULL, NULL)")
        self.source.commit()
        self.source.execute("ALTER TABLE notes ADD COLUMN extra TEXT")
        with self.assertRaisesRegex(ChangefeedError, "discard_history=True"):
            self.source_feed.refresh_table("notes")
        self.source_feed.refresh_table("notes", discard_history=True)
        self.assertEqual(self.source_feed.read_events(), ())
        self.source.execute("INSERT INTO notes VALUES (2, 'new', NULL, NULL, 'v2')")
        event = self.source_feed.read_events()[0]
        self.assertEqual(event.row["extra"], "v2")

    def test_event_and_batch_validation(self):
        with self.assertRaisesRegex(ChangefeedError, "valid JSON"):
            ChangeEvent.from_json("{")
        event = self.source_feed.read_events()
        self.assertEqual(event, ())
        with self.assertRaisesRegex(ValueError, "limit"):
            self.source_feed.read_events(limit=0)
        base = {"feedId": "source", "seq": 1, "table": "notes", "operation": "D", "key": {"id": 1}, "row": None}
        self.assertEqual(ChangeEvent.from_dict(base).operation, "D")
        with self.assertRaisesRegex(ChangefeedError, "strictly increasing"):
            self.target_feed.apply_events([ChangeEvent.from_dict(base), ChangeEvent.from_dict(base)])

    def test_wire_blob_marker_is_validated(self):
        with self.assertRaisesRegex(ChangefeedError, "blob encoding"):
            ChangeEvent.from_dict({"feedId": "f", "seq": 1, "table": "t", "operation": "D",
                                  "key": {"id": {"$sqlite_blob_hex": "xyz"}}, "row": None})

    def test_initialization_does_not_commit_an_existing_caller_transaction(self):
        connection = sqlite3.connect(":memory:")
        connection.execute("CREATE TABLE pending (id INTEGER PRIMARY KEY, body TEXT)")
        connection.commit()
        connection.execute("BEGIN")
        connection.execute("INSERT INTO pending VALUES (1, 'uncommitted')")
        self.assertTrue(connection.in_transaction)
        feed = Changefeed(connection)
        feed.register_table("pending")
        self.assertTrue(connection.in_transaction)
        connection.rollback()
        self.assertEqual(connection.execute("SELECT COUNT(*) FROM pending").fetchone()[0], 0)
        connection.close()

    def test_cloned_database_can_rotate_source_identity(self):
        self.source.execute("INSERT INTO notes VALUES (1, 'seed', NULL, NULL)")
        self.source.commit()
        clone = sqlite3.connect(":memory:")
        self.source.backup(clone)
        clone_feed = Changefeed(clone)
        self.assertEqual(clone_feed.feed_id, self.source_feed.feed_id)
        clone_feed.rotate_identity("device-copy")
        self.assertEqual(clone_feed.feed_id, "device-copy")
        self.assertNotEqual(clone_feed.feed_id, self.source_feed.feed_id)
        self.assertEqual(clone_feed.read_events(), ())
        clone.execute("INSERT INTO notes VALUES (2, 'local', NULL, NULL)")
        self.assertEqual(clone_feed.read_events()[0].feed_id, "device-copy")
        clone.close()


if __name__ == "__main__":
    unittest.main()
