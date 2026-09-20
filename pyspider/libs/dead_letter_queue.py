#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# vim: set et sw=4 ts=4 sts=4 ff=unix fenc=utf8:

"""
Dead letter queue for tasks that exhausted their retry budget.

Two backends are provided:

* :class:`MemoryDeadLetterQueue` - bounded in-memory deque (zero deps,
  handy for tests and one-off runs).
* :class:`SqliteDeadLetterQueue` - persistent SQLite storage living in
  the scheduler ``data_path``.

A task enters the queue through :meth:`BaseDeadLetterQueue.put` and can be
inspected (:meth:`list`/:meth:`get`), discarded (:meth:`pop`) or put back
into circulation (:meth:`requeue_task`).
"""

import json
import logging
import os
import sqlite3
import time
from collections import deque

from six import iteritems

logger = logging.getLogger('dead_letter_queue')


class DeadLetterEntry(dict):
    """A dead letter record (behaves like a plain dict)."""

    @classmethod
    def from_task(cls, task, error_type, reason='', retried=0):
        return cls({
            'id': '%s:%s' % (task.get('project'), task.get('taskid')),
            'project': task.get('project'),
            'taskid': task.get('taskid'),
            'url': task.get('url'),
            'error_type': error_type,
            'reason': reason,
            'retried': retried,
            'dead_time': time.time(),
            'lastcrawltime': task.get('lastcrawltime'),
            'task': dict(task),
        })


class BaseDeadLetterQueue(object):
    """Abstract dead letter queue."""

    def put(self, task, error_type, reason='', retried=0):
        raise NotImplementedError

    def size(self, project=None):
        raise NotImplementedError

    def list(self, limit=100, project=None):
        raise NotImplementedError

    def get(self, entry_id):
        raise NotImplementedError

    def pop(self, entry_id):
        raise NotImplementedError

    def requeue_task(self, entry):
        """Build an ACTIVE task from a dead letter entry."""
        task = dict(entry.get('task') or {})
        schedule = dict(task.get('schedule') or {})
        schedule.pop('retried', None)
        schedule['exetime'] = time.time()
        schedule['dead_letter_requeued'] = True
        task['schedule'] = schedule
        task['status'] = None  # caller sets the concrete taskdb status
        return task


class MemoryDeadLetterQueue(BaseDeadLetterQueue):
    """Bounded in-memory dead letter queue; oldest entries are evicted."""

    def __init__(self, max_size=1000):
        self.max_size = max_size
        self._entries = {}
        self._order = deque()

    def put(self, task, error_type, reason='', retried=0):
        entry = DeadLetterEntry.from_task(task, error_type, reason, retried)
        entry_id = entry['id']
        if entry_id not in self._entries:
            self._order.append(entry_id)
        self._entries[entry_id] = entry
        while len(self._order) > self.max_size:
            evicted_id = self._order.popleft()
            self._entries.pop(evicted_id, None)
            logger.warning('dead letter queue full, evicted %s', evicted_id)
        return entry

    def size(self, project=None):
        if project is None:
            return len(self._entries)
        return sum(1 for entry in self._entries.values()
                   if entry.get('project') == project)

    def list(self, limit=100, project=None):
        result = []
        for entry_id in reversed(self._order):
            entry = self._entries.get(entry_id)
            if entry is None:
                continue
            if project is not None and entry.get('project') != project:
                continue
            result.append(entry)
            if len(result) >= limit:
                break
        return result

    def get(self, entry_id):
        return self._entries.get(entry_id)

    def pop(self, entry_id):
        entry = self._entries.pop(entry_id, None)
        try:
            self._order.remove(entry_id)
        except ValueError:
            pass
        return entry


class SqliteDeadLetterQueue(BaseDeadLetterQueue):
    """Persistent dead letter storage backed by a SQLite file."""

    schema = """
    CREATE TABLE IF NOT EXISTS dead_letter (
        id TEXT PRIMARY KEY,
        project TEXT,
        taskid TEXT,
        url TEXT,
        error_type TEXT,
        reason TEXT,
        retried INTEGER,
        dead_time REAL,
        lastcrawltime REAL,
        task TEXT
    );
    CREATE INDEX IF NOT EXISTS idx_dead_letter_project
        ON dead_letter(project);
    """

    def __init__(self, path, max_size=1000):
        self.path = path
        self.max_size = max_size
        dirname = os.path.dirname(path)
        if dirname and not os.path.exists(dirname):
            os.makedirs(dirname)
        self.conn = sqlite3.connect(path)
        self.conn.executescript(self.schema)
        self.conn.commit()

    def _row_to_entry(self, row):
        try:
            task = json.loads(row[9]) if row[9] else {}
        except (TypeError, ValueError):
            task = {}
        return DeadLetterEntry({
            'id': row[0],
            'project': row[1],
            'taskid': row[2],
            'url': row[3],
            'error_type': row[4],
            'reason': row[5],
            'retried': row[6],
            'dead_time': row[7],
            'lastcrawltime': row[8],
            'task': task,
        })

    def put(self, task, error_type, reason='', retried=0):
        entry = DeadLetterEntry.from_task(task, error_type, reason, retried)
        self.conn.execute(
            'INSERT OR REPLACE INTO dead_letter (id, project, taskid, url, '
            'error_type, reason, retried, dead_time, lastcrawltime, task) '
            'VALUES (?,?,?,?,?,?,?,?,?,?)',
            (
                entry['id'], entry['project'], entry['taskid'], entry['url'],
                entry['error_type'], entry['reason'], entry['retried'],
                entry['dead_time'], entry['lastcrawltime'],
                json.dumps(entry['task']),
            )
        )
        self.conn.commit()

        if self.max_size:
            count = self.conn.execute(
                'SELECT COUNT(*) FROM dead_letter').fetchone()[0]
            if count > self.max_size:
                overflow = count - self.max_size
                self.conn.execute(
                    'DELETE FROM dead_letter WHERE id IN ('
                    '  SELECT id FROM dead_letter ORDER BY dead_time ASC '
                    '  LIMIT ?)', (overflow, ))
                self.conn.commit()
                logger.warning('dead letter queue full, evicted %d entries',
                               overflow)
        return entry

    def size(self, project=None):
        if project is None:
            return self.conn.execute(
                'SELECT COUNT(*) FROM dead_letter').fetchone()[0]
        return self.conn.execute(
            'SELECT COUNT(*) FROM dead_letter WHERE project=?',
            (project, )).fetchone()[0]

    def list(self, limit=100, project=None):
        if project is None:
            rows = self.conn.execute(
                'SELECT id, project, taskid, url, error_type, reason, '
                'retried, dead_time, lastcrawltime, task FROM dead_letter '
                'ORDER BY dead_time DESC LIMIT ?', (limit, )).fetchall()
        else:
            rows = self.conn.execute(
                'SELECT id, project, taskid, url, error_type, reason, '
                'retried, dead_time, lastcrawltime, task FROM dead_letter '
                'WHERE project=? ORDER BY dead_time DESC LIMIT ?',
                (project, limit)).fetchall()
        return [self._row_to_entry(row) for row in rows]

    def get(self, entry_id):
        row = self.conn.execute(
            'SELECT id, project, taskid, url, error_type, reason, '
            'retried, dead_time, lastcrawltime, task FROM dead_letter '
            'WHERE id=?', (entry_id, )).fetchone()
        if row is None:
            return None
        return self._row_to_entry(row)

    def pop(self, entry_id):
        entry = self.get(entry_id)
        if entry is not None:
            self.conn.execute('DELETE FROM dead_letter WHERE id=?',
                              (entry_id, ))
            self.conn.commit()
        return entry

    def close(self):
        try:
            self.conn.close()
        except Exception:
            pass
