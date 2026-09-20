#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# vim: set et sw=4 ts=4 sts=4 ff=unix fenc=utf8:
'''
Unified error classification and retry policy.

This module is the single place shared by scheduler / fetcher / processor
to decide:
  - which category an error belongs to (network / parse / business / unknown)
  - whether a failed task should be retried, and after how long
  - when a task is exhausted and should be moved to the dead letter queue

It intentionally has no third-party dependencies so it can be used and
tested standalone.
'''

import random


class ErrorCategory(object):
    '''Unified error categories.'''
    NETWORK = 'network'    # timeout, connection reset, 5xx, dns, proxy...
    PARSE = 'parse'        # html/json/xml parse, encoding, selector errors
    BUSINESS = 'business'  # 4xx, handler raised business exceptions
    UNKNOWN = 'unknown'

    ALL = (NETWORK, PARSE, BUSINESS, UNKNOWN)


class NetworkError(Exception):
    '''Raise in a handler to mark a failure as a network error.'''


class ParseError(Exception):
    '''Raise in a handler to mark a failure as a parse error.'''


class BusinessError(Exception):
    '''Raise in a handler to mark a failure as a business error.'''


_EXCEPTION_CATEGORY_MAP = (
    (NetworkError, ErrorCategory.NETWORK),
    (ParseError, ErrorCategory.PARSE),
    (BusinessError, ErrorCategory.BUSINESS),
)

_NETWORK_HINTS = (
    'timeout', 'timedout', 'connection', 'socket', 'network',
    'dns', 'gaierror', 'refused', 'reset', 'httperror', 'urlerror',
    'proxy', 'ssl', 'eof',
)

_PARSE_HINTS = (
    'parse', 'json', 'xml', 'html', 'selector', 'xpath',
    'decode', 'encoding', 'unicode', 'syntax',
)


def _match_hints(exc, hints):
    name = '%s.%s' % (type(exc).__module__, type(exc).__name__)
    name = name.lower()
    text = '%s %s' % (name, exc)
    for hint in hints:
        if hint in text:
            return True
    return False


def classify_exception(exc):
    '''Classify an exception raised while processing a task.'''
    if exc is None:
        return None
    for exc_type, category in _EXCEPTION_CATEGORY_MAP:
        if isinstance(exc, exc_type):
            return category
    if _match_hints(exc, _NETWORK_HINTS):
        return ErrorCategory.NETWORK
    if _match_hints(exc, _PARSE_HINTS):
        return ErrorCategory.PARSE
    return ErrorCategory.UNKNOWN


def classify_fetch_error(status_code, error=None):
    '''Classify a fetch failure by status code and error message.'''
    try:
        status_code = int(status_code)
    except (TypeError, ValueError):
        status_code = 599
    if status_code >= 500 or status_code == 599:
        return ErrorCategory.NETWORK
    if 400 <= status_code < 500:
        return ErrorCategory.BUSINESS
    if error:
        text = str(error).lower()
        for hint in _NETWORK_HINTS:
            if hint in text:
                return ErrorCategory.NETWORK
    return ErrorCategory.UNKNOWN


class RetryPolicy(object):
    '''
    Per-category retry policy.

    config format::

        {
            'default': {
                'max_retries': 3,
                'backoff': 'exponential',   # fixed | linear | exponential
                'base_delay': 30,           # seconds
                'factor': 2.0,              # exponential factor
                'max_delay': 86400,         # cap of computed delay
                'jitter': 0,                # 0~1, random jitter ratio
            },
            'categories': {
                'network': {...},           # override default per category
                'parse': {...},
                'business': {...},
                'unknown': {...},
            },
        }

    A legacy retry-delay map (``{0: 30, 1: 3600, '': 86400}``) is also
    accepted via :meth:`from_legacy_delay_map` for backward compatibility.
    '''

    DEFAULT_CONFIG = {
        'max_retries': 3,
        'backoff': 'exponential',
        'base_delay': 30,
        'factor': 2.0,
        'max_delay': 24 * 60 * 60,
        'jitter': 0,
    }

    def __init__(self, config=None):
        config = config or {}
        default = dict(self.DEFAULT_CONFIG)
        default.update(config.get('default') or {})
        self._default = default
        self._categories = {}
        for category, override in (config.get('categories') or {}).items():
            merged = dict(self._default)
            merged.update(override or {})
            self._categories[category] = merged
        self._legacy_delay_map = None

    @classmethod
    def from_legacy_delay_map(cls, delay_map):
        '''Build a policy from a legacy ``{retried: seconds, '': fallback}`` map.'''
        policy = cls()
        policy._legacy_delay_map = dict(delay_map or {})
        return policy

    def category_config(self, category):
        return self._categories.get(category, self._default)

    def max_retries(self, category, retries=None):
        '''Effective max retries. Task level ``retries`` takes precedence.'''
        if retries is not None:
            return retries
        return self.category_config(category).get('max_retries', 3)

    def should_retry(self, category, retried, retries=None):
        return retried < self.max_retries(category, retries)

    def next_delay(self, category, retried, retries=None):
        '''
        Seconds to wait before the next retry, or None when the task is
        exhausted and should be moved to the dead letter queue.
        '''
        if not self.should_retry(category, retried, retries):
            return None
        if self._legacy_delay_map is not None:
            fallback = self._legacy_delay_map.get('', 24 * 60 * 60)
            return self._legacy_delay_map.get(retried, fallback)
        config = self.category_config(category)
        base = float(config.get('base_delay', 30))
        backoff = config.get('backoff', 'exponential')
        if backoff == 'fixed':
            delay = base
        elif backoff == 'linear':
            delay = base * (retried + 1)
        else:  # exponential
            factor = float(config.get('factor', 2.0))
            delay = base * (factor ** retried)
        max_delay = config.get('max_delay')
        if max_delay is not None:
            delay = min(delay, float(max_delay))
        jitter = float(config.get('jitter') or 0)
        if jitter > 0:
            delay += delay * jitter * random.random()
        return delay
