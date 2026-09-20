#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# vim: set et sw=4 ts=4 sts=4 ff=unix fenc=utf8:

"""
Unit tests for the unified error taxonomy, retry policies and the dead
letter queue (error-handling / retry refactor).
"""

import os
import shutil
import time
import unittest

from pyspider.libs.error_handling import (
    ErrorType, TaskError, NetworkError, ParseError, BusinessError,
    SystemError, classify_exception, classify_status_code,
    classify_error_message, classify_fetch_error, classify_task_error,
)
from pyspider.libs.retry import (
    RetryStrategy, FixedDelay, ExponentialBackoff, CustomDelay,
    build_strategy, RetryPolicy, build_policy,
)
from pyspider.libs.retry.policy import LEGACY_RETRY_DELAY
from pyspider.libs.dead_letter_queue import (
    DeadLetterEntry, MemoryDeadLetterQueue, SqliteDeadLetterQueue,
)


class TestErrorClassification(unittest.TestCase):

    def test_status_code_classification(self):
        self.assertEqual(classify_status_code(200), None)
        self.assertEqual(classify_status_code(302), None)
        self.assertEqual(classify_status_code(404), ErrorType.BUSINESS)
        self.assertEqual(classify_status_code(403), ErrorType.BUSINESS)
        self.assertEqual(classify_status_code(500), ErrorType.NETWORK)
        self.assertEqual(classify_status_code(502), ErrorType.NETWORK)
        self.assertEqual(classify_status_code(599), ErrorType.NETWORK)
        self.assertEqual(classify_status_code(501), ErrorType.PARSE)
        self.assertEqual(classify_status_code(None), ErrorType.UNKNOWN)
        self.assertEqual(classify_status_code('not-a-code'), ErrorType.UNKNOWN)

    def test_task_error_subclasses(self):
        self.assertEqual(NetworkError().error_type, ErrorType.NETWORK)
        self.assertEqual(ParseError().error_type, ErrorType.PARSE)
        self.assertEqual(BusinessError().error_type, ErrorType.BUSINESS)
        self.assertEqual(SystemError().error_type, ErrorType.SYSTEM)
        self.assertEqual(TaskError('x', ErrorType.PARSE).error_type,
                         ErrorType.PARSE)

    def test_exception_classification(self):
        self.assertEqual(classify_exception(None), ErrorType.UNKNOWN)
        self.assertEqual(classify_exception(ValueError('bad json')),
                         ErrorType.PARSE)
        self.assertEqual(classify_exception(KeyError('field')),
                         ErrorType.PARSE)
        self.assertEqual(classify_exception(TimeoutError()),
                         ErrorType.NETWORK)
        try:
            raise ConnectionResetError('reset by peer')
        except ConnectionResetError as exc:
            self.assertEqual(classify_exception(exc), ErrorType.NETWORK)
        self.assertEqual(classify_exception(NetworkError()),
                         ErrorType.NETWORK)
        self.assertEqual(classify_exception(AssertionError()),
                         ErrorType.SYSTEM)
        self.assertEqual(classify_exception(RuntimeError('handler boom')),
                         ErrorType.BUSINESS)

    def test_error_message_classification(self):
        self.assertEqual(classify_error_message('connection timeout'),
                         ErrorType.NETWORK)
        self.assertEqual(classify_error_message('JSON decode failed'),
                         ErrorType.PARSE)
        self.assertEqual(classify_error_message(''), ErrorType.UNKNOWN)
        self.assertEqual(classify_error_message('something odd'),
                         ErrorType.UNKNOWN)

    def test_fetch_error_classification(self):
        self.assertEqual(classify_fetch_error(error='timeout', status_code=599),
                         ErrorType.NETWORK)
        self.assertEqual(classify_fetch_error(error=None, status_code=404),
                         ErrorType.BUSINESS)
        self.assertEqual(classify_fetch_error(error='not found', status_code=500),
                         ErrorType.NETWORK)
        self.assertEqual(classify_fetch_error(error='connection refused'),
                         ErrorType.NETWORK)
        self.assertEqual(classify_fetch_error(error='weird', status_code=599),
                         ErrorType.NETWORK)
        self.assertEqual(classify_fetch_error(status_code=200),
                         ErrorType.UNKNOWN)
        self.assertEqual(classify_fetch_error(error=NetworkError()),
                         ErrorType.NETWORK)

    def test_classify_task_error_explicit_stamp(self):
        task = {'track': {
            'fetch': {'ok': False, 'status_code': 500,
                      'error_type': ErrorType.NETWORK},
            'process': {'ok': False, 'error_type': ErrorType.PARSE},
        }}
        self.assertEqual(classify_task_error(task), ErrorType.PARSE)

    def test_classify_task_error_fallback_status(self):
        task = {'track': {
            'fetch': {'ok': False, 'status_code': 404, 'error': 'Not Found'},
            'process': {'ok': True},
        }}
        self.assertEqual(classify_task_error(task), ErrorType.BUSINESS)

    def test_classify_task_error_network(self):
        task = {'track': {
            'fetch': {'ok': False, 'status_code': 599, 'error': 'timeout'},
            'process': {'ok': False},
        }}
        self.assertEqual(classify_task_error(task), ErrorType.NETWORK)

    def test_classify_task_error_callback_traceback(self):
        task = {'track': {
            'fetch': {'ok': True, 'status_code': 200},
            'process': {'ok': False,
                        'logs': 'Traceback (most recent call last):\nboom'},
        }}
        self.assertEqual(classify_task_error(task), ErrorType.BUSINESS)

    def test_classify_task_error_empty(self):
        self.assertEqual(classify_task_error({}), ErrorType.UNKNOWN)
        self.assertEqual(classify_task_error(None), ErrorType.UNKNOWN)


class TestRetryStrategy(unittest.TestCase):

    def test_fixed_delay(self):
        strategy = FixedDelay(42)
        self.assertEqual(strategy.get_delay(0), 42)
        self.assertEqual(strategy.get_delay(9), 42)

    def test_exponential_backoff(self):
        strategy = ExponentialBackoff(base_delay=10, multiplier=2, max_delay=100)
        self.assertEqual(strategy.get_delay(0), 10)
        self.assertEqual(strategy.get_delay(1), 20)
        self.assertEqual(strategy.get_delay(2), 40)
        self.assertEqual(strategy.get_delay(3), 80)
        self.assertEqual(strategy.get_delay(4), 100)

    def test_exponential_backoff_jitter(self):
        strategy = ExponentialBackoff(base_delay=10, jitter=5)
        for idx in range(10):
            self.assertGreaterEqual(strategy.get_delay(idx), 10.0)

    def test_custom_delay_table(self):
        strategy = CustomDelay({0: 30, 1: 60}, default_delay=300)
        self.assertEqual(strategy.get_delay(0), 30)
        self.assertEqual(strategy.get_delay(1), 60)
        self.assertEqual(strategy.get_delay(2), 300)

    def test_custom_delay_empty_key(self):
        strategy = CustomDelay({'': 99})
        self.assertEqual(strategy.get_delay(5), 99)

    def test_build_strategy_shapes(self):
        self.assertIsInstance(build_strategy(15), FixedDelay)
        self.assertIsInstance(
            build_strategy({'type': 'fixed', 'delay': 5}), FixedDelay)
        self.assertIsInstance(
            build_strategy({'type': 'exponential', 'base_delay': 1}),
            ExponentialBackoff)
        self.assertIsInstance(build_strategy(None), ExponentialBackoff)
        legacy = build_strategy({0: 30, 1: 60, '': 100})
        self.assertIsInstance(legacy, CustomDelay)
        self.assertEqual(legacy.get_delay(2), 100)
        strategy = build_strategy(lambda retried: retried * 7)
        self.assertEqual(strategy.get_delay(3), 21)
        with self.assertRaises(ValueError):
            build_strategy({'type': 'no-such-type'})
        with self.assertRaises(ValueError):
            build_strategy(object())


class TestRetryPolicy(unittest.TestCase):

    def test_should_retry(self):
        policy = RetryPolicy(max_retries=2, strategy=10)
        self.assertTrue(policy.should_retry(0))
        self.assertTrue(policy.should_retry(1))
        self.assertFalse(policy.should_retry(2))
        self.assertFalse(policy.should_retry(3))

    def test_no_retry(self):
        policy = RetryPolicy(max_retries=0, strategy=10)
        self.assertFalse(policy.should_retry(0))

    def test_default_policies(self):
        policies = build_policy()
        for category in ErrorType.ALL:
            self.assertIn(category, policies)
            self.assertIsInstance(policies[category], RetryPolicy)
        self.assertEqual(policies[ErrorType.NETWORK].get_delay(1),
                         LEGACY_RETRY_DELAY[1])
        self.assertLess(policies[ErrorType.BUSINESS].max_retries,
                        policies[ErrorType.NETWORK].max_retries)
        self.assertGreater(policies[ErrorType.PARSE].max_retries, 0)

    def test_build_policy_global_override(self):
        policies = build_policy(
            {'retries': 7, 'strategy': {'type': 'fixed', 'delay': 12}})
        for category in ErrorType.ALL:
            self.assertEqual(policies[category].max_retries, 7)
            self.assertEqual(policies[category].get_delay(3), 12)

    def test_build_policy_per_category(self):
        policies = build_policy({
            ErrorType.NETWORK: {
                'max_retries': 9,
                'strategy': {'type': 'exponential', 'base_delay': 2},
            },
            ErrorType.BUSINESS: {'max_retries': 0, 'strategy': 0},
        })
        self.assertEqual(policies[ErrorType.NETWORK].max_retries, 9)
        self.assertEqual(policies[ErrorType.NETWORK].get_delay(2), 8)
        self.assertEqual(policies[ErrorType.BUSINESS].max_retries, 0)
        self.assertGreater(policies[ErrorType.PARSE].max_retries, 0)

    def test_build_policy_default_section(self):
        policies = build_policy({
            'default': {'max_retries': 4, 'strategy': 5},
            ErrorType.PARSE: {'max_retries': 1, 'strategy': 1},
        })
        self.assertEqual(policies[ErrorType.NETWORK].max_retries, 4)
        self.assertEqual(policies[ErrorType.PARSE].max_retries, 1)

    def test_build_policy_legacy_table(self):
        policies = build_policy({0: 11, 1: 22, '': 33}, default_retries=5)
        for category in ErrorType.ALL:
            self.assertEqual(policies[category].max_retries, 5)
            self.assertEqual(policies[category].get_delay(0), 11)
            self.assertEqual(policies[category].get_delay(2), 33)

    def test_build_policy_bad_input(self):
        with self.assertRaises(ValueError):
            build_policy(42)
        with self.assertRaises(ValueError):
            build_policy({ErrorType.NETWORK: object()})


class TestDeadLetterEntry(unittest.TestCase):

    def test_entry_from_task(self):
        task = {'project': 'p', 'taskid': 't', 'url': 'http://x/',
                'lastcrawltime': 1}
        entry = DeadLetterEntry.from_task(task, ErrorType.NETWORK,
                                          reason='boom', retried=3)
        self.assertEqual(entry['id'], 'p:t')
        self.assertEqual(entry['project'], 'p')
        self.assertEqual(entry['error_type'], ErrorType.NETWORK)
        self.assertEqual(entry['reason'], 'boom')
        self.assertEqual(entry['retried'], 3)
        self.assertEqual(entry['task']['taskid'], 't')


class TestMemoryDeadLetterQueue(unittest.TestCase):

    def setUp(self):
        self.dlq = MemoryDeadLetterQueue(max_size=3)

    def _task(self, taskid):
        return {'project': 'p', 'taskid': taskid, 'url': 'u/%s' % taskid}

    def test_put_and_size(self):
        self.dlq.put(self._task('a'), ErrorType.NETWORK, retried=3)
        self.assertEqual(self.dlq.size(), 1)
        self.assertEqual(self.dlq.size('p'), 1)
        self.assertEqual(self.dlq.size('other'), 0)

    def test_list_newest_first(self):
        for taskid in ('a', 'b', 'c'):
            self.dlq.put(self._task(taskid), ErrorType.NETWORK)
        self.assertEqual([e['taskid'] for e in self.dlq.list()],
                         ['c', 'b', 'a'])

    def test_get_and_pop(self):
        self.dlq.put(self._task('a'), ErrorType.BUSINESS)
        self.assertIsNotNone(self.dlq.get('p:a'))
        self.assertIsNone(self.dlq.get('p:nope'))
        self.assertIsNotNone(self.dlq.pop('p:a'))
        self.assertEqual(self.dlq.size(), 0)
        self.assertIsNone(self.dlq.pop('p:a'))

    def test_max_size_eviction(self):
        for idx in range(5):
            self.dlq.put(self._task(str(idx)), ErrorType.NETWORK)
        self.assertEqual(self.dlq.size(), 3)
        self.assertEqual(
            [e['taskid'] for e in self.dlq.list(limit=10)],
            ['4', '3', '2'])

    def test_upsert_same_task(self):
        self.dlq.put(self._task('a'), ErrorType.NETWORK, retried=1)
        self.dlq.put(self._task('a'), ErrorType.BUSINESS, retried=4)
        self.assertEqual(self.dlq.size(), 1)
        self.assertEqual(self.dlq.get('p:a')['error_type'],
                         ErrorType.BUSINESS)

    def test_requeue_task_resets_retries(self):
        task = self._task('a')
        task['schedule'] = {'retried': 3, 'exetime': 100}
        self.dlq.put(task, ErrorType.NETWORK, retried=3)
        entry = self.dlq.get('p:a')
        requeued = self.dlq.requeue_task(entry)
        self.assertNotIn('retried', requeued['schedule'])
        self.assertTrue(requeued['schedule']['dead_letter_requeued'])
        self.assertGreaterEqual(requeued['schedule']['exetime'], time.time() - 1)


class TestSqliteDeadLetterQueue(unittest.TestCase):

    path = '/tmp/pyspider_dlq_test/dead_letter.db'

    def setUp(self):
        shutil.rmtree('/tmp/pyspider_dlq_test', ignore_errors=True)
        self.dlq = SqliteDeadLetterQueue(self.path, max_size=3)

    def tearDown(self):
        self.dlq.close()
        shutil.rmtree('/tmp/pyspider_dlq_test', ignore_errors=True)

    def test_persistence_roundtrip(self):
        task = {'project': 'p', 'taskid': 'a', 'url': 'http://x/',
                'schedule': {'retried': 3}}
        self.dlq.put(task, ErrorType.PARSE, reason='bad html', retried=3)
        reloaded = SqliteDeadLetterQueue(self.path)
        self.assertEqual(reloaded.size(), 1)
        entry = reloaded.get('p:a')
        self.assertEqual(entry['error_type'], ErrorType.PARSE)
        self.assertEqual(entry['task']['schedule']['retried'], 3)
        reloaded.close()

    def test_list_filter_and_pop(self):
        for project, taskid in (('p', 'a'), ('p', 'b'), ('q', 'c')):
            self.dlq.put({'project': project, 'taskid': taskid, 'url': 'u'},
                         ErrorType.NETWORK)
        self.assertEqual(self.dlq.size('p'), 2)
        self.assertEqual(len(self.dlq.list(project='q')), 1)
        self.assertEqual(len(self.dlq.list(limit=1)), 1)
        self.assertTrue(self.dlq.pop('p:a'))
        self.assertEqual(self.dlq.size(), 2)

    def test_max_size_eviction(self):
        for idx in range(5):
            self.dlq.put({'project': 'p', 'taskid': str(idx), 'url': 'u'},
                         ErrorType.NETWORK)
        self.assertEqual(self.dlq.size(), 3)
        self.assertEqual(
            sorted(e['taskid'] for e in self.dlq.list(limit=10)),
            ['2', '3', '4'])


from pyspider.scheduler.scheduler import Scheduler, Project
from pyspider.database.sqlite import taskdb as sqlite_taskdb
from six.moves import queue as SixQueue


class SchedulerRetryTestMixin(object):
    """Direct on_task_failed / on_task_status tests without sockets."""

    taskdb_path = '/tmp/pyspider_error_test/task.db'

    def make_scheduler(self, retry_policy=None, dead_letter_queue='memory',
                       inqueue_limit=0):
        shutil.rmtree('/tmp/pyspider_error_test', ignore_errors=True)
        os.makedirs('/tmp/pyspider_error_test')
        self.taskdb = sqlite_taskdb.TaskDB(self.taskdb_path)
        self.newtask_queue = SixQueue.Queue()
        self.status_queue = SixQueue.Queue()
        self.out_queue = SixQueue.Queue()
        scheduler = Scheduler(
            taskdb=self.taskdb, projectdb=None,
            newtask_queue=self.newtask_queue, status_queue=self.status_queue,
            out_queue=self.out_queue, data_path='/tmp/pyspider_error_test',
            retry_policy=retry_policy, dead_letter_queue=dead_letter_queue)
        scheduler.INQUEUE_LIMIT = inqueue_limit
        project = Project(scheduler, {
            'name': 'test_project',
            'group': 'group',
            'status': 'RUNNING',
            'script': '',
            'rate': 1.0,
            'burst': 10,
            'updatetime': time.time(),
        })
        project.waiting_get_info = False
        scheduler.projects['test_project'] = project
        return scheduler, project

    def fail_pack(self, retried=0, retries=3, fetch_ok=False,
                  status_code=599, error='timeout', process_ok=False,
                  error_type=None, schedule_extra=None):
        pack = {
            'taskid': 'taskid',
            'project': 'test_project',
            'url': 'url',
            'schedule': {
                'retried': retried,
                'retries': retries,
            },
            'track': {
                'fetch': {
                    'ok': fetch_ok,
                    'status_code': status_code,
                    'error': error,
                },
                'process': {
                    'ok': process_ok,
                },
            },
        }
        if error_type is not None:
            pack['track']['process']['error_type'] = error_type
        if schedule_extra:
            pack['schedule'].update(schedule_extra)
        return pack

    def seed_active_task(self, scheduler, taskid='taskid'):
        task = {
            'taskid': taskid,
            'project': 'test_project',
            'url': 'url',
            'status': scheduler.taskdb.ACTIVE,
            'schedule': {
                'retried': 0,
                'retries': 3,
            },
        }
        scheduler.insert_task(task)
        scheduler.put_task(task)
        task_queue = scheduler.projects['test_project'].task_queue
        task_queue.rate = 100000
        task_queue.burst = 100000
        task_queue.bucket.set(100000)
        scheduler.projects['test_project'].task_queue.check_update()
        self.assertIsNotNone(task_queue.get())
        return task


class TestSchedulerRetryClassification(SchedulerRetryTestMixin,
                                       unittest.TestCase):

    def tearDown(self):
        shutil.rmtree('/tmp/pyspider_error_test', ignore_errors=True)

    def test_network_retry_uses_policy_delay(self):
        scheduler, project = self.make_scheduler(
            retry_policy={'retries': 3,
                          'strategy': {'type': 'fixed', 'delay': 50}})
        self.seed_active_task(scheduler)
        before = time.time()
        scheduler.on_task_failed(
            self.fail_pack(retried=0, status_code=599, error='timeout'))
        db_task = scheduler.taskdb.get_task('test_project', 'taskid')
        self.assertEqual(db_task['status'], scheduler.taskdb.ACTIVE)
        self.assertEqual(db_task['schedule']['retried'], 1)
        self.assertAlmostEqual(db_task['schedule']['exetime'] - before,
                               50, delta=5)
        self.assertEqual(db_task['schedule']['error_type'],
                         ErrorType.NETWORK)

    def test_business_error_independent_policy(self):
        scheduler, project = self.make_scheduler(retry_policy={
            ErrorType.NETWORK: {'max_retries': 1, 'strategy': 100},
            ErrorType.BUSINESS: {'max_retries': 5, 'strategy': 1},
        })
        self.seed_active_task(scheduler)
        # a 404 failure still retries (business allows 5)
        scheduler.on_task_failed(
            self.fail_pack(retried=0, status_code=404, error='Not Found'))
        db_task = scheduler.taskdb.get_task('test_project', 'taskid')
        self.assertEqual(db_task['schedule']['retried'], 1)
        self.assertEqual(db_task['schedule']['error_type'],
                         ErrorType.BUSINESS)
        # exhaust business budget -> dead letter
        scheduler.on_task_failed(
            self.fail_pack(retried=5, status_code=404, error='Not Found'))
        db_task = scheduler.taskdb.get_task('test_project', 'taskid')
        self.assertEqual(db_task['status'], scheduler.taskdb.FAILED)
        entry = scheduler.dead_letter_queue.get('test_project:taskid')
        self.assertIsNotNone(entry)
        self.assertEqual(entry['error_type'], ErrorType.BUSINESS)

    def test_network_error_retries_more_than_business(self):
        scheduler, project = self.make_scheduler()
        self.seed_active_task(scheduler)
        default_policies = scheduler.retry_policies
        self.assertGreater(
            default_policies[ErrorType.NETWORK].max_retries,
            default_policies[ErrorType.BUSINESS].max_retries)

    def test_parse_error_classification(self):
        scheduler, project = self.make_scheduler(retry_policy={
            ErrorType.PARSE: {'max_retries': 2, 'strategy': 0},
        })
        self.seed_active_task(scheduler)
        pack = self.fail_pack(retried=0)
        pack['track']['process']['ok'] = False
        pack['track']['process']['logs'] = \
            'ValueError: No JSON object could be decoded'
        pack['track']['process']['exception'] = \
            str(ValueError('No JSON object could be decoded'))
        scheduler.on_task_failed(pack)
        db_task = scheduler.taskdb.get_task('test_project', 'taskid')
        self.assertEqual(db_task['schedule']['error_type'],
                         ErrorType.PARSE)

    def test_explicit_error_type_stamp_respected(self):
        scheduler, project = self.make_scheduler(retry_policy={
            ErrorType.SYSTEM: {'max_retries': 0, 'strategy': 0},
        })
        self.seed_active_task(scheduler)
        pack = self.fail_pack(retried=0, status_code=200,
                              error_type=ErrorType.SYSTEM)
        scheduler.on_task_failed(pack)
        self.assertEqual(scheduler.dead_letter_queue.size(), 1)
        entry = scheduler.dead_letter_queue.list()[0]
        self.assertEqual(entry['error_type'], ErrorType.SYSTEM)


class TestDeadLetterFlow(SchedulerRetryTestMixin, unittest.TestCase):

    def tearDown(self):
        shutil.rmtree('/tmp/pyspider_error_test', ignore_errors=True)

    def test_exhausted_task_moves_to_dlq_not_dropped(self):
        scheduler, project = self.make_scheduler(retry_policy={
            ErrorType.NETWORK: {'max_retries': 1, 'strategy': 0},
        })
        self.seed_active_task(scheduler)
        scheduler.on_task_failed(self.fail_pack(retried=0))
        # pull the requeued task out of the time queue so it re-enters
        # processing
        scheduler.projects['test_project'].task_queue.get()
        scheduler.on_task_failed(self.fail_pack(retried=1))

        db_task = scheduler.taskdb.get_task('test_project', 'taskid')
        self.assertEqual(db_task['status'], scheduler.taskdb.FAILED)
        self.assertEqual(scheduler.dead_letter_queue.size(), 1)
        entry = scheduler.dead_letter_queue.get('test_project:taskid')
        self.assertIsNotNone(entry)
        self.assertEqual(entry['task']['url'], 'url')

    def test_dlq_requeue_reactivates_task(self):
        scheduler, project = self.make_scheduler(retry_policy={
            ErrorType.NETWORK: {'max_retries': 0, 'strategy': 0},
        })
        self.seed_active_task(scheduler)
        scheduler.on_task_failed(self.fail_pack(retried=0))
        self.assertEqual(scheduler.dead_letter_queue.size(), 1)
        entry = scheduler.dead_letter_queue.get('test_project:taskid')

        task = scheduler.dead_letter_queue.requeue_task(entry)
        task['status'] = scheduler.taskdb.ACTIVE
        scheduler.update_task(task)
        scheduler.put_task(task)
        scheduler.dead_letter_queue.pop(entry['id'])

        self.assertEqual(scheduler.dead_letter_queue.size(), 0)
        db_task = scheduler.taskdb.get_task('test_project', 'taskid')
        self.assertEqual(db_task['status'], scheduler.taskdb.ACTIVE)
        self.assertTrue(db_task['schedule'].get('dead_letter_requeued'))
        self.assertEqual(db_task['schedule'].get('retried', 0), 0)
        self.assertEqual(len(scheduler.projects['test_project'].task_queue), 1)

    def test_dlq_disabled_marks_failed(self):
        scheduler, project = self.make_scheduler(retry_policy={
            ErrorType.NETWORK: {'max_retries': 0, 'strategy': 0},
        }, dead_letter_queue=False)
        self.assertIsNone(scheduler.dead_letter_queue)
        self.seed_active_task(scheduler)
        scheduler.on_task_failed(self.fail_pack(retried=0))
        db_task = scheduler.taskdb.get_task('test_project', 'taskid')
        self.assertEqual(db_task['status'], scheduler.taskdb.FAILED)

    def test_sqlite_dlq_backend(self):
        scheduler, project = self.make_scheduler(retry_policy={
            ErrorType.NETWORK: {'max_retries': 0, 'strategy': 0},
        }, dead_letter_queue='sqlite')
        self.seed_active_task(scheduler)
        scheduler.on_task_failed(self.fail_pack(retried=0))
        self.assertEqual(scheduler.dead_letter_queue.size(), 1)
        scheduler.dead_letter_queue.close()

    def test_per_task_retry_policy_override(self):
        scheduler, project = self.make_scheduler(retry_policy={
            ErrorType.NETWORK: {'max_retries': 0, 'strategy': 0},
        })
        self.seed_active_task(scheduler)
        pack = self.fail_pack(
            retried=0,
            schedule_extra={
                'retry_policy': {
                    ErrorType.NETWORK: {'max_retries': 5, 'strategy': 0},
                },
            })
        scheduler.on_task_failed(pack)
        db_task = scheduler.taskdb.get_task('test_project', 'taskid')
        self.assertEqual(db_task['status'], scheduler.taskdb.ACTIVE)
        self.assertEqual(db_task['schedule']['retried'], 1)
        self.assertEqual(scheduler.dead_letter_queue.size(), 0)

    def test_per_task_retries_mapping(self):
        scheduler, project = self.make_scheduler(retry_policy={
            ErrorType.NETWORK: {'max_retries': 9, 'strategy': 0},
        })
        self.seed_active_task(scheduler)
        pack = self.fail_pack(
            retried=0,
            schedule_extra={
                'retries': {ErrorType.NETWORK: 0, '': 3},
            })
        scheduler.on_task_failed(pack)
        self.assertEqual(scheduler.dead_letter_queue.size(), 1)


class TestProjectRetryConfig(SchedulerRetryTestMixin, unittest.TestCase):

    def tearDown(self):
        shutil.rmtree('/tmp/pyspider_error_test', ignore_errors=True)

    def test_project_level_retry_policy(self):
        scheduler, project = self.make_scheduler()
        self.seed_active_task(scheduler)
        project.on_get_info({
            'min_tick': 0,
            'retry_policy': {
                ErrorType.NETWORK: {'max_retries': 8, 'strategy': 7},
            },
            'crawl_config': {},
        })
        policies = project.get_retry_policy({})
        self.assertEqual(policies[ErrorType.NETWORK].max_retries, 8)
        self.assertEqual(policies[ErrorType.NETWORK].get_delay(0), 7)

    def test_project_legacy_retry_delay_still_works(self):
        scheduler, project = self.make_scheduler()
        self.seed_active_task(scheduler)
        project.on_get_info({
            'min_tick': 0,
            'retry_delay': {0: 12, 1: 34, '': 56},
            'crawl_config': {},
        })
        policies = project.get_retry_policy({})
        self.assertEqual(policies[ErrorType.NETWORK].get_delay(0), 12)
        self.assertEqual(policies[ErrorType.NETWORK].get_delay(1), 34)
        self.assertEqual(policies[ErrorType.NETWORK].get_delay(2), 56)

    def test_project_falls_back_to_scheduler_policy(self):
        scheduler, project = self.make_scheduler(
            retry_policy={'retries': 2, 'strategy': 9})
        self.seed_active_task(scheduler)
        project.on_get_info({'min_tick': 0, 'crawl_config': {}})
        policies = project.get_retry_policy({})
        self.assertEqual(policies[ErrorType.NETWORK].max_retries, 2)
        self.assertEqual(policies[ErrorType.NETWORK].get_delay(0), 9)


class TestPauseStateConsistency(SchedulerRetryTestMixin, unittest.TestCase):
    """Dead-lettered tasks are real failures for the pause state machine."""

    def tearDown(self):
        shutil.rmtree('/tmp/pyspider_error_test', ignore_errors=True)

    def test_dead_letter_counts_as_failure_for_pause(self):
        scheduler, project = self.make_scheduler(retry_policy={
            ErrorType.NETWORK: {'max_retries': 0, 'strategy': 0},
        })
        scheduler.FAIL_PAUSE_NUM = 3
        scheduler.PAUSE_TIME = 3600
        for idx in range(scheduler.FAIL_PAUSE_NUM):
            taskid = 'task_%d' % idx
            self.seed_active_task(scheduler, taskid)
            pack = {
                'taskid': taskid,
                'project': 'test_project',
                'url': 'url',
                'schedule': {'retried': 0, 'retries': 0},
                'track': {
                    'fetch': {'ok': False, 'status_code': 599, 'error': 'down'},
                    'process': {'ok': False},
                },
            }
            scheduler.on_task_status(pack)
        self.assertEqual(scheduler.dead_letter_queue.size(), 3)
        self.assertTrue(project.paused)

    def test_paused_project_keeps_retrying_tasks(self):
        scheduler, project = self.make_scheduler(retry_policy={
            ErrorType.NETWORK: {'max_retries': 3, 'strategy': 0},
        })
        scheduler.FAIL_PAUSE_NUM = 1
        scheduler.PAUSE_TIME = 3600
        self.seed_active_task(scheduler)
        scheduler.on_task_status(self.fail_pack(retried=0))
        self.assertTrue(project.paused)
        db_task = scheduler.taskdb.get_task('test_project', 'taskid')
        self.assertEqual(db_task['status'], scheduler.taskdb.ACTIVE)
        self.assertEqual(db_task['schedule']['retried'], 1)


if __name__ == '__main__':
    unittest.main()
