About Tasks
===========

Tasks are the basic unit to be scheduled.

Basis
-----

* A task is differentiated by its `taskid`. (Default: `md5(url)`, can be changed by overriding the `def get_taskid(self, task)` method)
* Tasks are isolated between different projects.
* A Task has 4 status:
    - active
    - failed
    - success
    - bad - not used
* Only tasks in active status will be scheduled.
* Tasks are served in order of `priority`.

Schedule
--------

#### new task

When a new task (never seen before) comes in:

* If `exetime` is set but not arrived, it will be put into a time-based queue to wait.
* Otherwise it will be accepted.

When the task is already in the queue:

* Ignored unless `force_update`

When a completed task comes out:

* If `age` is set, `last_crawl_time + age < now` it will be accepted. Otherwise discarded.
* If `itag` is set and not equal to it's previous value, it will be accepted. Otherwise discarded.


#### task retry

When a fetch error or script error happens, the task will retry 3 times by default.

The first retry will execute every time after 30 seconds, 1 hour, 6 hours, 12 hours and any more retries will postpone 24 hours.

If `age` is specified, the retry delay will not larger then `age`.

You can config the retry delay by adding a variable named `retry_delay` to handler. `retry_delay` is a dict to specify retry intervals. The items in the dict are {retried: seconds}, and a special key: '' (empty string) is used to specify the default retry delay if not specified.

e.g. the default `retry_delay` declares like:


```
class MyHandler(BaseHandler):
    retry_delay = {
        0: 30,
        1: 1*60*60,
        2: 6*60*60,
        3: 12*60*60,
        '': 24*60*60
    }
```

#### error categories and retry policy

Errors are classified into unified categories: `network` (timeout, connection, 5xx),
`parse` (html/json/xml parse errors) and `business` (4xx, or `BusinessError` raised
by the handler). You may raise `pyspider.libs.error_policy.NetworkError` /
`ParseError` / `BusinessError` in a handler to mark the category explicitly.

Each category can have its own retry strategy. Instead of the legacy delay map
above, `retry_delay` also accepts a policy config:

```
class MyHandler(BaseHandler):
    retry_delay = {
        'default': {
            'max_retries': 3,
            'backoff': 'exponential',  # fixed | linear | exponential
            'base_delay': 30,
            'factor': 2.0,
            'max_delay': 24*60*60,
            'jitter': 0,
        },
        'categories': {
            'network': {'max_retries': 5, 'base_delay': 30},
            'parse': {'max_retries': 1, 'backoff': 'fixed', 'base_delay': 3600},
            'business': {'max_retries': 0},
        },
    }
```

The scheduler wide default can be set with `Scheduler.RETRY_POLICY` in the same
format. A task level `retries` in `self.crawl` always takes precedence over
`max_retries`.

#### dead letter queue

When a task exceeds its max retries, it is moved into the dead letter queue
instead of being dropped silently. The task is marked `FAILED` in taskdb and a
record (taskid, project, url, error category, retried times, error) is kept by
the scheduler (persisted to `data/scheduler.deadletter`, latest
`Scheduler.DEAD_LETTER_LIMIT` records). Records can be queried and cleared via
the scheduler XMLRPC methods `get_dead_letters(project=None, limit=100)` and
`clear_dead_letters(project=None)`.
