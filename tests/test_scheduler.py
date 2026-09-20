#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# vim: set et sw=4 ts=4 sts=4 ff=unix fenc=utf8:
# Author: Binux<i@binux.me>
#         http://binux.me
# Created on 2014-02-08 22:37:13

import os
import time
import shutil
import unittest
import logging
import logging.config
logging.config.fileConfig("pyspider/logging.conf")

from pyspider.scheduler.task_queue import TaskQueue
from pyspider.libs import utils


class TestTaskQueue(unittest.TestCase):

    @classmethod
    def setUpClass(self):
        self.task_queue = TaskQueue()
        self.task_queue.rate = 100000
        self.task_queue.burst = 100000
        self.task_queue.processing_timeout = 0.5

    def test_10_put(self):
        self.task_queue.put('a3', 0, time.time() + 0.5)
        self.task_queue.put('a4', 3, time.time() + 0.2)
        self.task_queue.put('a2', 0)
        self.task_queue.put('a1', 1)
        self.assertEqual(self.task_queue.size(), 4)

    def test_20_update(self):
        self.task_queue.put('a2', 4)
        self.assertEqual(self.task_queue.size(), 4)
        self.task_queue.put('a3', 2, 0)
        self.assertEqual(self.task_queue.size(), 4)

    def test_30_get_from_priority_queue(self):
        self.assertEqual(self.task_queue.get(), 'a2')
        self.assertEqual(self.task_queue.size(), 4)

    def test_40_time_queue_1(self):
        self.task_queue.check_update()
        self.assertEqual(self.task_queue.get(), 'a3')
        self.assertEqual(self.task_queue.size(), 4)

    def test_50_time_queue_2(self):
        time.sleep(0.3)
        self.task_queue.check_update()
        self.assertEqual(self.task_queue.get(), 'a4')
        self.assertEqual(self.task_queue.get(), 'a1')
        self.assertEqual(self.task_queue.size(), 4)

    def test_60_processing_queue(self):
        time.sleep(0.5)
        self.task_queue.check_update()
        self.assertEqual(self.task_queue.get(), 'a2')
        self.assertEqual(len(self.task_queue), 4)
        self.assertEqual(self.task_queue.get(), 'a4')
        self.assertEqual(self.task_queue.get(), 'a3')
        self.assertEqual(self.task_queue.get(), 'a1')
        self.assertEqual(len(self.task_queue), 4)

    def test_70_done(self):
        self.assertTrue(self.task_queue.done('a2'))
        self.assertTrue(self.task_queue.done('a1'))
        self.assertEqual(len(self.task_queue), 2)
        self.assertTrue(self.task_queue.done('a4'))
        self.assertTrue(self.task_queue.done('a3'))
        self.assertEqual(len(self.task_queue), 0)


from pyspider.scheduler.token_bucket import Bucket


class TestBucket(unittest.TestCase):

    def test_bucket(self):
        bucket = Bucket(100, 1000)
        self.assertEqual(bucket.get(), 1000)
        time.sleep(0.1)
        self.assertEqual(bucket.get(), 1000)
        bucket.desc(100)
        self.assertEqual(bucket.get(), 900)
        time.sleep(0.1)
        self.assertAlmostEqual(bucket.get(), 910, delta=2)
        time.sleep(0.1)
        self.assertAlmostEqual(bucket.get(), 920, delta=2)


try:
    from six.moves import xmlrpc_client
except ImportError:
    import xmlrpclib as xmlrpc_client
from pyspider.scheduler.scheduler import Scheduler
from pyspider.database.sqlite import taskdb, projectdb, resultdb
from pyspider.libs.multiprocessing_queue import Queue
from pyspider.libs.utils import run_in_thread


class TestScheduler(unittest.TestCase):
    taskdb_path = './data/tests/task.db'
    projectdb_path = './data/tests/project.db'
    resultdb_path = './data/tests/result.db'
    check_project_time = 1
    scheduler_xmlrpc_port = 23333

    @classmethod
    def setUpClass(self):
        shutil.rmtree('./data/tests', ignore_errors=True)
        os.makedirs('./data/tests')

        def get_taskdb():
            return taskdb.TaskDB(self.taskdb_path)
        self.taskdb = get_taskdb()

        def get_projectdb():
            return projectdb.ProjectDB(self.projectdb_path)
        self.projectdb = get_projectdb()

        def get_resultdb():
            return resultdb.ResultDB(self.resultdb_path)
        self.resultdb = get_resultdb()

        self.newtask_queue = Queue(10)
        self.status_queue = Queue(10)
        self.scheduler2fetcher = Queue(10)
        self.rpc = xmlrpc_client.ServerProxy('http://localhost:%d' % self.scheduler_xmlrpc_port)

        def run_scheduler():
            scheduler = Scheduler(taskdb=get_taskdb(), projectdb=get_projectdb(),
                                  newtask_queue=self.newtask_queue, status_queue=self.status_queue,
                                  out_queue=self.scheduler2fetcher, data_path="./data/tests/",
                                  resultdb=get_resultdb())
            scheduler.UPDATE_PROJECT_INTERVAL = 0.1
            scheduler.LOOP_INTERVAL = 0.1
            scheduler.INQUEUE_LIMIT = 10
            scheduler.DELETE_TIME = 0
            scheduler.DEFAULT_RETRY_DELAY = {'': 5}
            scheduler._last_tick = int(time.time())  # not dispatch cronjob
            self.xmlrpc_thread = run_in_thread(scheduler.xmlrpc_run, port=self.scheduler_xmlrpc_port)
            scheduler.run()

        self.process = run_in_thread(run_scheduler)
        time.sleep(1)

    @classmethod
    def tearDownClass(self):
        if self.process.is_alive():
            self.rpc._quit()
            self.process.join(5)
        self.xmlrpc_thread.join()
        assert not self.process.is_alive()
        shutil.rmtree('./data/tests', ignore_errors=True)
        time.sleep(1)

        assert not utils.check_port_open(5000)
        assert not utils.check_port_open(self.scheduler_xmlrpc_port)
        assert not utils.check_port_open(24444)
        assert not utils.check_port_open(25555)

    def test_10_new_task_ignore(self):
        '''
        task_queue = [ ]
        '''
        self.newtask_queue.put({
            'taskid': 'taskid',
            'project': 'test_project',
            'url': 'url'
        })  # unknown project: test_project
        self.assertEqual(self.rpc.size(), 0)
        self.assertEqual(len(self.rpc.get_active_tasks()), 0)

    def test_20_new_project(self):
        '''
        task_queue = [ ]
        '''
        self.projectdb.insert('test_project', {
            'name': 'test_project',
            'group': 'group',
            'status': 'TODO',
            'script': 'import time\nprint(time.time())',
            'comments': 'test project',
            'rate': 1.0,
            'burst': 10,
        })

    def test_30_update_project(self):
        '''
        task_queue = [ ]
        '''
        from six.moves import queue as Queue
        with self.assertRaises(Queue.Empty):
            task = self.scheduler2fetcher.get(timeout=1)
        self.projectdb.update('test_project', status="DEBUG")
        time.sleep(0.1)
        self.rpc.update_project()

        task = self.scheduler2fetcher.get(timeout=10)
        self.assertIsNotNone(task)
        self.assertEqual(task['taskid'], '_on_get_info')  # select test_project:_on_get_info data:,_on_get_info

    def test_32_get_info(self):
        self.status_queue.put({
            'taskid': '_on_get_info',
            'project': 'test_project',
            'track': {
                'save': {
                    }
                }
            })
        # test_project on_get_info {}

    def test_34_new_not_used_project(self):
        '''
        task_queue = []
        '''
        self.projectdb.insert('test_project_not_started', {
            'name': 'test_project_not_started',
            'group': 'group',
            'status': 'RUNNING',
            'script': 'import time\nprint(time.time())',
            'comments': 'test project',
            'rate': 1.0,
            'burst': 10,
        })
        task = self.scheduler2fetcher.get(timeout=5)  # select test_project_not_started:_on_get_info data:,_on_get_info
        self.assertEqual(task['taskid'], '_on_get_info')

    def test_35_new_task(self):
        '''
        task_queue = [ ]
        '''
        time.sleep(0.2)
        self.newtask_queue.put({
            'taskid': 'taskid',
            'project': 'test_project',
            'url': 'url',
            'fetch': {
                'data': 'abc',
            },
            'process': {
                'data': 'abc',
            },
            'schedule': {
                'age': 0,
            },
        })  # new task test_project:taskid url
        # task_queue = [ test_project:taskid ]

        time.sleep(0.5)
        task = self.scheduler2fetcher.get(timeout=10)  # select test_project:taskid
        self.assertGreater(len(self.rpc.get_active_tasks()), 0)
        self.assertIsNotNone(task)
        self.assertEqual(task['taskid'], 'taskid')
        self.assertEqual(task['project'], 'test_project')
        self.assertIn('schedule', task)
        self.assertIn('fetch', task)
        self.assertIn('process', task)
        self.assertIn('track', task)
        self.assertEqual(task['fetch']['data'], 'abc')

    def test_37_force_update_processing_task(self):
        '''
        processing = [ test_project:taskid ]
        '''
        self.newtask_queue.put({
            'taskid': 'taskid',
            'project': 'test_project',
            'url': 'url_force_update',
            'schedule': {
                'age': 10,
                'force_update': True,
            },
        })  # restart task test_project:taskid url_force_update
        time.sleep(0.2)
        # it should not block next

    def test_40_taskdone_error_no_project(self):
        '''
        processing = [ test_project:taskid ]
        '''
        self.status_queue.put({
            'taskid': 'taskid',
            'project': 'no_project',
            'url': 'url'
        })  # unknown project: no_project
        time.sleep(0.1)
        self.assertEqual(self.rpc.size(), 1)

    def test_50_taskdone_error_no_track(self):
        '''
        processing = [ test_project:taskid ]
        '''
        self.status_queue.put({
            'taskid': 'taskid',
            'project': 'test_project',
            'url': 'url'
        })  # Bad status pack: 'track'
        time.sleep(0.1)
        self.assertEqual(self.rpc.size(), 1)
        self.status_queue.put({
            'taskid': 'taskid',
            'project': 'test_project',
            'url': 'url',
            'track': {}
        })  # Bad status pack: 'process'
        time.sleep(0.1)
        self.assertEqual(self.rpc.size(), 1)

    def test_60_taskdone_failed_retry(self):
        '''
        processing = [ test_project:taskid ]
        '''
        self.status_queue.put({
            'taskid': 'taskid',
            'project': 'test_project',
            'url': 'url',
            'track': {
                'fetch': {
                    'ok': True
                },
                'process': {
                    'ok': False
                },
            }
        })  # task retry 0/3 test_project:taskid url
        from six.moves import queue as Queue
        # with self.assertRaises(Queue.Empty):
            # task = self.scheduler2fetcher.get(timeout=4)
        task = self.scheduler2fetcher.get(timeout=5)  # select test_project:taskid url
        self.assertIsNotNone(task)

    def test_70_taskdone_ok(self):
        '''
        processing = [ test_project:taskid ]
        '''
        self.status_queue.put({
            'taskid': 'taskid',
            'project': 'test_project',
            'url': 'url',
            'track': {
                'fetch': {
                    'ok': True
                },
                'process': {
                    'ok': True
                },
            }
        })  # task done test_project:taskid url
        time.sleep(0.2)
        self.assertEqual(self.rpc.size(), 0)

    def test_75_on_finished_msg(self):
        task = self.scheduler2fetcher.get(timeout=5)  # select test_project:on_finished data:,on_finished

        self.assertEqual(task['taskid'], 'on_finished')

        self.status_queue.put({
            'taskid': 'on_finished',
            'project': 'test_project',
            'url': 'url',
            'track': {
                'fetch': {
                    'ok': True
                },
                'process': {
                    'ok': True
                },
            }
        })  # task done test_project:on_finished url
        time.sleep(0.2)
        self.assertEqual(self.rpc.size(), 0)

    def test_80_newtask_age_ignore(self):
        '''
        processing = [ ]
        '''
        self.newtask_queue.put({
            'taskid': 'taskid',
            'project': 'test_project',
            'url': 'url',
            'fetch': {
                'data': 'abc',
            },
            'process': {
                'data': 'abc',
            },
            'schedule': {
                'age': 30,
            },
        })
        time.sleep(0.1)
        self.assertEqual(self.rpc.size(), 0)

    def test_82_newtask_via_rpc(self):
        '''
        processing = [ ]
        '''
        self.rpc.newtask({
            'taskid': 'taskid',
            'project': 'test_project',
            'url': 'url',
            'fetch': {
                'data': 'abc',
            },
            'process': {
                'data': 'abc',
            },
            'schedule': {
                'age': 30,
            },
        })
        time.sleep(0.1)
        self.assertEqual(self.rpc.size(), 0)

    def test_90_newtask_with_itag(self):
        '''
        task_queue = [ ]
        processing = [ ]
        '''
        time.sleep(0.1)
        self.newtask_queue.put({
            'taskid': 'taskid',
            'project': 'test_project',
            'url': 'url',
            'fetch': {
                'data': 'abc',
            },
            'process': {
                'data': 'abc',
            },
            'schedule': {
                'itag': "abc",
                'retries': 1
            },
        })  # restart task test_project:taskid url

        task = self.scheduler2fetcher.get(timeout=10)  # select test_project:taskid url
        self.assertIsNotNone(task)
        self.assertEqual(task['taskid'], 'taskid')

        self.test_70_taskdone_ok()  # task done test_project:taskid url
        self.test_75_on_finished_msg()  # select test_project:on_finished data:,on_finished

    def test_a10_newtask_restart_by_age(self):
        self.newtask_queue.put({
            'taskid': 'taskid',
            'project': 'test_project',
            'url': 'url',
            'fetch': {
                'data': 'abc',
            },
            'process': {
                'data': 'abc',
            },
            'schedule': {
                'age': 0,
                'retries': 1
            },
        })  # restart task test_project:taskid url
        task = self.scheduler2fetcher.get(timeout=10)  # select test_project:taskid url
        self.assertIsNotNone(task)
        self.assertEqual(task['taskid'], 'taskid')

    def test_a20_failed_retry(self):
        '''
        processing: [ test_project:taskid ]
        '''
        self.status_queue.put({
            'taskid': 'taskid',
            'project': 'test_project',
            'url': 'url',
            'track': {
                'fetch': {
                    'ok': True
                },
                'process': {
                    'ok': False
                },
            }
        })  # task retry 0/1 test_project:taskid url
        task = self.scheduler2fetcher.get(timeout=5)  # select test_project:taskid url
        self.assertIsNotNone(task)
        self.assertEqual(task['taskid'], 'taskid')

        self.status_queue.put({
            'taskid': 'taskid',
            'project': 'test_project',
            'url': 'url',
            'track': {
                'fetch': {
                    'ok': False
                },
                'process': {
                    'ok': False
                },
            }
        })  # task failed test_project:taskid url

        self.test_75_on_finished_msg()  # select test_project:on_finished data:,on_finished

        from six.moves import queue as Queue
        with self.assertRaises(Queue.Empty):
            self.scheduler2fetcher.get(timeout=5)

    def test_a30_task_verify(self):
        self.assertFalse(self.rpc.newtask({
            #'taskid': 'taskid#',
            'project': 'test_project',
            'url': 'url',
        }))  # taskid not in task: {'project': 'test_project', 'url': 'url'}
        self.assertFalse(self.rpc.newtask({
            'taskid': 'taskid#',
            #'project': 'test_project',
            'url': 'url',
        }))  # project not in task: {'url': 'url', 'taskid': 'taskid#'}
        self.assertFalse(self.rpc.newtask({
            'taskid': 'taskid#',
            'project': 'test_project',
            #'url': 'url',
        }))  # url not in task: {'project': 'test_project', 'taskid': 'taskid#'}
        self.assertFalse(self.rpc.newtask({
            'taskid': 'taskid#',
            'project': 'not_exist_project',
            'url': 'url',
        }))  # unknown project: not_exist_project
        self.assertTrue(self.rpc.newtask({
            'taskid': 'taskid#',
            'project': 'test_project',
            'url': 'url',
        }))  # new task test_project:taskid# url

    def test_a40_success_recrawl(self):
        '''
        task_queue = [ test_project:taskid# ]
        '''
        self.newtask_queue.put({
            'taskid': 'taskid',
            'project': 'test_project',
            'url': 'url',
            'fetch': {
                'data': 'abc',
            },
            'process': {
                'data': 'abc',
            },
            'schedule': {
                'age': 0,
                'retries': 1,
                'auto_recrawl': True,
            },
        })  # restart task test_project:taskid url
        task1 = self.scheduler2fetcher.get(timeout=10)  # select test_project:taskid# url
        task2 = self.scheduler2fetcher.get(timeout=10)  # select test_project:taskid url
        self.assertIsNotNone(task1)
        self.assertIsNotNone(task2)
        self.assertTrue(task1['taskid'] == 'taskid#' or task2['taskid'] == 'taskid#')

        self.status_queue.put({
            'taskid': 'taskid',
            'project': 'test_project',
            'url': 'url',
            'schedule': {
                'age': 0,
                'retries': 1,
                'auto_recrawl': True,
            },
            'track': {
                'fetch': {
                    'ok': True
                },
                'process': {
                    'ok': True
                },
            }
        })  # task done test_project:taskid url
        task = self.scheduler2fetcher.get(timeout=10)
        self.assertIsNotNone(task)

    def test_a50_failed_recrawl(self):
        '''
        time_queue = [ test_project:taskid ]
        scheduler2fetcher = [ test_project:taskid# ]
        processing = [ test_project:taskid# ]
        '''
        for i in range(3):
            self.status_queue.put({
                'taskid': 'taskid',
                'project': 'test_project',
                'url': 'url',
                'schedule': {
                    'age': 0,
                    'retries': 1,
                    'auto_recrawl': True,
                },
                'track': {
                    'fetch': {
                        'ok': True
                    },
                    'process': {
                        'ok': False
                    },
                }
            })
            # not processing pack: test_project:taskid url
            # select test_project:taskid url
            # task retry 0/1 test_project:taskid url
            # select test_project:taskid url
            # task retry 0/1 test_project:taskid url
            # select test_project:taskid url
            task = self.scheduler2fetcher.get(timeout=10)
            self.assertIsNotNone(task)
            self.assertEqual(task['taskid'], 'taskid')

    def test_a60_disable_recrawl(self):
        '''
        time_queue = [ test_project:taskid ]
        scheduler2fetcher = [ test_project:taskid# ]
        processing = [ test_project:taskid# ]
        '''
        self.status_queue.put({
            'taskid': 'taskid',
            'project': 'test_project',
            'url': 'url',
            'schedule': {
                'age': 0,
                'retries': 1,
            },
            'track': {
                'fetch': {
                    'ok': True
                },
                'process': {
                    'ok': True
                },
            }
        })  # task done test_project:taskid url

        from six.moves import queue as Queue
        with self.assertRaises(Queue.Empty):
            self.scheduler2fetcher.get(timeout=5)

    def test_38_cancel_task(self):
        current_size = self.rpc.size()
        self.newtask_queue.put({
            'taskid': 'taskid_to_cancel',
            'project': 'test_project',
            'url': 'url',
            'fetch': {
                'data': 'abc',
            },
            'process': {
                'data': 'abc',
            },
            'schedule': {
                'age': 0,
                'exetime': time.time() + 30
            },
        })  # new task test_project:taskid_to_cancel url
        # task_queue = [ test_project:taskid_to_cancel ]

        time.sleep(0.2)
        self.assertEqual(self.rpc.size(), current_size+1)

        self.newtask_queue.put({
            'taskid': 'taskid_to_cancel',
            'project': 'test_project',
            'url': 'url',
            'fetch': {
                'data': 'abc',
            },
            'process': {
                'data': 'abc',
            },
            'schedule': {
                'force_update': True,
                'age': 0,
                'cancel': True
            },
        })  # new cancel test_project:taskid_to_cancel url
        # task_queue = [ ]

        time.sleep(0.2)
        self.assertEqual(self.rpc.size(), current_size)

    def test_x10_inqueue_limit(self):
        self.projectdb.insert('test_inqueue_project', {
            'name': 'test_inqueue_project',
            'group': 'group',
            'status': 'DEBUG',
            'script': 'import time\nprint(time.time())',
            'comments': 'test project',
            'rate': 0,
            'burst': 0,
        })
        time.sleep(0.1)
        pre_size = self.rpc.size()
        for i in range(20):
            self.newtask_queue.put({
                'taskid': 'taskid%d' % i,
                'project': 'test_inqueue_project',
                'url': 'url',
                'schedule': {
                    'age': 3000,
                    'force_update': True,
                },
            })
        time.sleep(1)
        self.assertEqual(self.rpc.size() - pre_size, 10)

    def test_x20_delete_project(self):
        self.assertIsNotNone(self.projectdb.get('test_inqueue_project'))
        #self.assertIsNotNone(self.taskdb.get_task('test_inqueue_project', 'taskid1'))
        self.projectdb.update('test_inqueue_project', status="STOP", group="lock,delete")
        time.sleep(1)
        self.assertIsNone(self.projectdb.get('test_inqueue_project'))
        self.taskdb._list_project()
        self.assertIsNone(self.taskdb.get_task('test_inqueue_project', 'taskid1'))
        self.assertNotIn('test_inqueue_project', self.rpc.counter('5m', 'sum'))

    def test_z10_startup(self):
        self.assertTrue(self.process.is_alive())

    def test_z20_quit(self):
        self.rpc._quit()
        time.sleep(0.2)
        self.assertFalse(self.process.is_alive())
        self.assertEqual(
            self.taskdb.get_task('test_project', 'taskid')['status'],
            self.taskdb.SUCCESS
        )


from pyspider.scheduler.scheduler import Project

class TestProject(unittest.TestCase):
    task_pack = {
        'type': Scheduler.TASK_PACK,
        'taskid': 'taskid',
        'project': 'test_project',
        'url': 'url',
        'fetch': {
            'data': 'abc',
        },
        'process': {
            'data': 'abc',
        },
        'schedule': {
            'age': 0,
        },
    }

    status_ok_pack = {
        'taskid': 'taskid',
        'project': 'test_project',
        'url': 'url',
        'schedule': {
            'age': 0,
            'retries': 1,
        },
        'track': {
            'fetch': {
                'ok': True
            },
            'process': {
                'ok': True
            },
        }
    }

    status_fail_pack = {
        'taskid': 'taskid',
        'project': 'test_project',
        'url': 'url',
        'schedule': {
            'age': 0,
            'retries': 1,
        },
        'track': {
            'fetch': {
                'ok': False
            },
            'process': {
                'ok': False
            },
        }
    }

    @classmethod
    def setUpClass(self):
        self.scheduler = Scheduler(taskdb=None, projectdb=None, newtask_queue=None, status_queue=None, out_queue=None)
        self.scheduler.PAUSE_TIME = 2
        self.project = Project(self.scheduler, {
            'name': 'test_project_not_started',
            'group': 'group',
            'status': 'RUNNING',
            'script': 'import time\nprint(time.time())',
            'comments': 'test project',
            'rate': 1.0,
            'burst': 10,
            'updatetime': time.time(),
        })

    def test_pause_10_unpaused(self):
        self.assertFalse(self.project.paused)

    def test_pause_20_no_enough_fail_tasks(self):
        for i in range(3):
            self.project.active_tasks.appendleft((time.time(), dict(self.task_pack)))
        self.assertFalse(self.project.paused)

        for i in range(1):
            self.project.active_tasks.appendleft((time.time(), dict(self.status_ok_pack)))
        for i in range(self.scheduler.FAIL_PAUSE_NUM - 5):
            self.project.active_tasks.appendleft((time.time(), dict(self.status_fail_pack)))
        self.assertFalse(self.project.paused)

        for i in range(5):
            self.project.active_tasks.appendleft((time.time(), dict(self.status_fail_pack)))
        for i in range(1):
            self.project.active_tasks.appendleft((time.time(), dict(self.status_ok_pack)))
        self.assertFalse(self.project.paused)

        for i in range(self.scheduler.FAIL_PAUSE_NUM):
            self.project.active_tasks.appendleft((time.time(), dict(self.task_pack)))
        self.assertFalse(self.project.paused)

    def test_pause_30_paused(self):
        for i in range(self.scheduler.FAIL_PAUSE_NUM):
            self.project.active_tasks.appendleft((time.time(), dict(self.status_fail_pack)))
        for i in range(self.scheduler.FAIL_PAUSE_NUM):
            self.project.active_tasks.appendleft((time.time(), dict(self.task_pack)))
        self.assertTrue(self.project.paused)

    def test_pause_40_unpause_checking(self):
        time.sleep(3)
        self.assertFalse(self.project.paused)

    def test_pause_50_paused_again(self):
        for i in range(self.scheduler.UNPAUSE_CHECK_NUM):
            self.project.active_tasks.appendleft((time.time(), dict(self.status_fail_pack)))
        self.assertTrue(self.project.paused)

    def test_pause_60_unpause_checking(self):
        time.sleep(3)
        self.assertFalse(self.project.paused)

    def test_pause_70_unpaused(self):
        for i in range(1):
            self.project.active_tasks.appendleft((time.time(), dict(self.status_ok_pack)))
        for i in range(self.scheduler.UNPAUSE_CHECK_NUM):
            self.project.active_tasks.appendleft((time.time(), dict(self.status_fail_pack)))
        for i in range(self.scheduler.FAIL_PAUSE_NUM):
            self.project.active_tasks.appendleft((time.time(), dict(self.task_pack)))
        self.assertFalse(self.project.paused)
        self.assertFalse(self.project._paused)

    def test_pause_x_disable_auto_pause(self):
        fail_pause_num = self.scheduler.FAIL_PAUSE_NUM
        self.scheduler.FAIL_PAUSE_NUM = 0
        for i in range(100):
            self.project.active_tasks.appendleft((time.time(), dict(self.status_fail_pack)))
        self.assertFalse(self.project.paused)
        self.scheduler.FAIL_PAUSE_NUM = fail_pause_num

    def test_pause_x_will_retry_not_counted(self):
        '''tasks waiting for retry should not trigger the paused state machine'''
        self.project._paused = False
        self.project.active_tasks.clear()
        will_retry_fail_pack = dict(self.status_fail_pack)
        will_retry_fail_pack['will_retry'] = True
        for i in range(self.scheduler.FAIL_PAUSE_NUM * 2):
            self.project.active_tasks.appendleft((time.time(), dict(will_retry_fail_pack)))
        self.assertFalse(self.project.paused)
        self.assertFalse(self.project._paused)
        # without the will_retry mark, the same packs do trigger pause
        self.project.active_tasks.clear()
        for i in range(self.scheduler.FAIL_PAUSE_NUM):
            self.project.active_tasks.appendleft((time.time(), dict(self.status_fail_pack)))
        self.assertTrue(self.project.paused)
        self.project._paused = False
        self.project.active_tasks.clear()


class TestDeadLetter(unittest.TestCase):
    '''retry policy / dead letter queue integration with Scheduler.on_task_failed'''
    data_path = './data/tests_deadletter'

    def setUp(self):
        shutil.rmtree(self.data_path, ignore_errors=True)
        os.makedirs(self.data_path)
        self.taskdb = taskdb.TaskDB(os.path.join(self.data_path, 'task.db'))
        self.projectdb = projectdb.ProjectDB(os.path.join(self.data_path, 'project.db'))
        self.resultdb = resultdb.ResultDB(os.path.join(self.data_path, 'result.db'))
        self.scheduler = Scheduler(
            taskdb=self.taskdb, projectdb=self.projectdb,
            newtask_queue=Queue(10), status_queue=Queue(10),
            out_queue=Queue(10), data_path=self.data_path,
            resultdb=self.resultdb)
        self.scheduler.DEFAULT_RETRY_DELAY = {'': 0}
        self.scheduler._update_project({
            'name': 'test_project',
            'group': 'group',
            'status': 'RUNNING',
            'script': '',
            'comments': '',
            'rate': 100000,
            'burst': 100000,
            'updatetime': time.time(),
        })
        self.project = self.scheduler.projects['test_project']
        self.project.on_get_info({})

    def tearDown(self):
        shutil.rmtree(self.data_path, ignore_errors=True)

    def _submit_and_select(self, taskid='taskid', schedule=None):
        task = {
            'taskid': taskid,
            'project': 'test_project',
            'url': 'http://example.com/%s' % taskid,
        }
        if schedule:
            task['schedule'] = schedule
        self.scheduler.on_new_request(task)
        self.project.task_queue.check_update()
        self.assertEqual(self.project.task_queue.get(), taskid)

    def _fail(self, taskid='taskid', fetch_ok=True, status_code=200,
              error=None, exception_type=None):
        track = {
            'fetch': {
                'ok': fetch_ok,
                'status_code': status_code,
                'error': error,
                'time': 0.01,
            },
            'process': {
                'ok': False,
                'time': 0.01,
            },
        }
        if exception_type:
            track['process']['exception_type'] = exception_type
        self.scheduler.on_task_status({
            'taskid': taskid,
            'project': 'test_project',
            'url': 'http://example.com/%s' % taskid,
            'track': track,
        })

    def test_10_retry_then_dead_letter(self):
        self._submit_and_select(schedule={'retries': 1})
        self._fail()  # retried 0/1 -> retry
        # retried task is marked will_retry for the paused state machine
        self.assertTrue(self.project.active_tasks[0][1].get('will_retry'))
        # and requeued
        self.project.task_queue.check_update()
        self.assertEqual(self.project.task_queue.get(), 'taskid')

        self._fail()  # retried 1/1 -> exhausted -> dead letter
        self.assertEqual(len(self.scheduler._dead_letters), 1)
        record = self.scheduler._dead_letters[0]
        self.assertEqual(record['taskid'], 'taskid')
        self.assertEqual(record['project'], 'test_project')
        self.assertEqual(record['category'], 'unknown')
        self.assertEqual(record['retried'], 1)
        # task marked FAILED in taskdb, not dropped
        dbtask = self.taskdb.get_task('test_project', 'taskid')
        self.assertEqual(dbtask['status'], self.taskdb.FAILED)
        # persisted to data_path
        dead_letter_file = os.path.join(self.data_path, 'scheduler.deadletter')
        self.assertTrue(os.path.exists(dead_letter_file))
        with open(dead_letter_file) as fp:
            lines = [x for x in fp.read().splitlines() if x]
        self.assertEqual(len(lines), 1)

    def test_20_fetch_error_category(self):
        self._submit_and_select(schedule={'retries': 0})
        self._fail(fetch_ok=False, status_code=599, error='Connection refused')
        self.assertEqual(len(self.scheduler._dead_letters), 1)
        self.assertEqual(self.scheduler._dead_letters[0]['category'], 'network')

    def test_30_process_exception_category(self):
        self._submit_and_select(schedule={'retries': 0})
        self._fail(exception_type='parse')
        self.assertEqual(len(self.scheduler._dead_letters), 1)
        self.assertEqual(self.scheduler._dead_letters[0]['category'], 'parse')

    def test_40_retry_policy_config(self):
        self.scheduler.RETRY_POLICY = {
            'default': {
                'max_retries': 5,
                'backoff': 'exponential',
                'base_delay': 100,
                'factor': 2.0,
            },
            'categories': {
                'business': {'max_retries': 0},
            },
        }
        # business error: no retry at all, straight to dead letter
        self._submit_and_select(taskid='t1')
        self._fail(taskid='t1', fetch_ok=False, status_code=404)
        self.assertEqual(len(self.scheduler._dead_letters), 1)
        self.assertEqual(self.scheduler._dead_letters[0]['category'], 'business')

        # network error: exponential backoff, 100s for the first retry
        self._submit_and_select(taskid='t2')
        before = time.time()
        self._fail(taskid='t2', fetch_ok=False, status_code=599)
        dbtask = self.taskdb.get_task('test_project', 't2')
        self.assertEqual(dbtask['schedule']['retried'], 1)
        delay = dbtask['schedule']['exetime'] - before
        self.assertGreater(delay, 90)
        self.assertLessEqual(delay, 101)
        self.assertEqual(len(self.scheduler._dead_letters), 1)

    def test_50_legacy_project_retry_delay(self):
        self.project.retry_delay = {0: 7, '': 99}
        policy = self.scheduler._get_retry_policy(self.project)
        self.assertEqual(policy.next_delay('network', 0, 3), 7)
        self.assertEqual(policy.next_delay('network', 1, 3), 99)
        self.assertIsNone(policy.next_delay('network', 3, 3))

    def test_60_get_and_clear_dead_letters(self):
        self._submit_and_select(taskid='t1', schedule={'retries': 0})
        self._fail(taskid='t1')
        self._submit_and_select(taskid='t2', schedule={'retries': 0})
        self._fail(taskid='t2', fetch_ok=False, status_code=500)

        records = self.scheduler.get_dead_letters()
        self.assertEqual(len(records), 2)
        # newest first
        self.assertEqual(records[0]['taskid'], 't2')
        records = self.scheduler.get_dead_letters(project='test_project', limit=1)
        self.assertEqual(len(records), 1)
        records = self.scheduler.get_dead_letters(project='no_such_project')
        self.assertEqual(len(records), 0)

        self.assertEqual(self.scheduler.clear_dead_letters(), 2)
        self.assertEqual(len(self.scheduler._dead_letters), 0)
        dead_letter_file = os.path.join(self.data_path, 'scheduler.deadletter')
        with open(dead_letter_file) as fp:
            self.assertEqual(fp.read(), '')


if __name__ == '__main__':
    unittest.main()
