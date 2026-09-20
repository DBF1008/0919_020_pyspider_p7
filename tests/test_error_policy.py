#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# vim: set et sw=4 ts=4 sts=4 ff=unix fenc=utf8:

import unittest

from pyspider.libs.error_policy import (
    ErrorCategory,
    NetworkError,
    ParseError,
    BusinessError,
    RetryPolicy,
    classify_exception,
    classify_fetch_error,
)


class TestClassifyException(unittest.TestCase):

    def test_10_explicit_categories(self):
        self.assertEqual(classify_exception(NetworkError('timeout')),
                         ErrorCategory.NETWORK)
        self.assertEqual(classify_exception(ParseError('bad html')),
                         ErrorCategory.PARSE)
        self.assertEqual(classify_exception(BusinessError('login required')),
                         ErrorCategory.BUSINESS)

    def test_20_network_hints(self):
        self.assertEqual(classify_exception(Exception('Connection refused')),
                         ErrorCategory.NETWORK)
        self.assertEqual(classify_exception(TimeoutError('timed out')),
                         ErrorCategory.NETWORK)

    def test_30_parse_hints(self):
        self.assertEqual(
            classify_exception(ValueError('No JSON object could be decoded')),
            ErrorCategory.PARSE)

    def test_40_unknown(self):
        self.assertEqual(classify_exception(Exception('something')), 
                         ErrorCategory.UNKNOWN)
        self.assertIsNone(classify_exception(None))


class TestClassifyFetchError(unittest.TestCase):

    def test_10_network(self):
        self.assertEqual(classify_fetch_error(599), ErrorCategory.NETWORK)
        self.assertEqual(classify_fetch_error(502), ErrorCategory.NETWORK)
        self.assertEqual(classify_fetch_error(500), ErrorCategory.NETWORK)

    def test_20_business(self):
        self.assertEqual(classify_fetch_error(404), ErrorCategory.BUSINESS)
        self.assertEqual(classify_fetch_error(403), ErrorCategory.BUSINESS)

    def test_30_error_text_hint(self):
        self.assertEqual(
            classify_fetch_error(599, Exception('Could not resolve host')),
            ErrorCategory.NETWORK)

    def test_40_unknown(self):
        self.assertEqual(classify_fetch_error(200), ErrorCategory.UNKNOWN)
        self.assertEqual(classify_fetch_error(None), ErrorCategory.NETWORK)


class TestRetryPolicy(unittest.TestCase):

    def test_10_fixed_backoff(self):
        policy = RetryPolicy({'default': {
            'max_retries': 3, 'backoff': 'fixed', 'base_delay': 10,
        }})
        self.assertEqual(policy.next_delay('network', 0), 10)
        self.assertEqual(policy.next_delay('network', 1), 10)
        self.assertEqual(policy.next_delay('network', 2), 10)
        self.assertIsNone(policy.next_delay('network', 3))

    def test_20_linear_backoff(self):
        policy = RetryPolicy({'default': {
            'max_retries': 3, 'backoff': 'linear', 'base_delay': 10,
        }})
        self.assertEqual(policy.next_delay('network', 0), 10)
        self.assertEqual(policy.next_delay('network', 1), 20)
        self.assertEqual(policy.next_delay('network', 2), 30)

    def test_30_exponential_backoff(self):
        policy = RetryPolicy({'default': {
            'max_retries': 4, 'backoff': 'exponential',
            'base_delay': 30, 'factor': 2.0,
        }})
        self.assertEqual(policy.next_delay('network', 0), 30)
        self.assertEqual(policy.next_delay('network', 1), 60)
        self.assertEqual(policy.next_delay('network', 2), 120)
        self.assertEqual(policy.next_delay('network', 3), 240)
        self.assertIsNone(policy.next_delay('network', 4))

    def test_40_max_delay_cap(self):
        policy = RetryPolicy({'default': {
            'max_retries': 10, 'backoff': 'exponential',
            'base_delay': 30, 'factor': 2.0, 'max_delay': 100,
        }})
        self.assertEqual(policy.next_delay('network', 5), 100)

    def test_50_per_category_config(self):
        policy = RetryPolicy({
            'default': {'max_retries': 3, 'backoff': 'fixed', 'base_delay': 10},
            'categories': {
                'parse': {'max_retries': 1, 'base_delay': 3600},
                'business': {'max_retries': 0},
            },
        })
        self.assertEqual(policy.next_delay('parse', 0), 3600)
        self.assertIsNone(policy.next_delay('parse', 1))
        self.assertIsNone(policy.next_delay('business', 0))
        self.assertEqual(policy.next_delay('network', 0), 10)

    def test_60_task_level_retries_override(self):
        policy = RetryPolicy({'default': {
            'max_retries': 1, 'backoff': 'fixed', 'base_delay': 10,
        }})
        # task level retries takes precedence
        self.assertEqual(policy.next_delay('network', 2, retries=5), 10)
        self.assertIsNone(policy.next_delay('network', 5, retries=5))
        # policy max_retries used when task level retries not set
        self.assertIsNone(policy.next_delay('network', 1))

    def test_70_legacy_delay_map(self):
        delay_map = {0: 30, 1: 3600, 2: 21600, '': 86400}
        policy = RetryPolicy.from_legacy_delay_map(delay_map)
        self.assertEqual(policy.next_delay('network', 0, retries=3), 30)
        self.assertEqual(policy.next_delay('network', 1, retries=3), 3600)
        self.assertEqual(policy.next_delay('network', 2, retries=3), 21600)
        # fallback to '' for missing key
        self.assertEqual(policy.next_delay('unknown', 9, retries=10), 86400)
        # exhausted
        self.assertIsNone(policy.next_delay('network', 3, retries=3))
        # category does not matter for legacy map
        self.assertEqual(policy.next_delay('parse', 0, retries=3), 30)

    def test_80_jitter(self):
        policy = RetryPolicy({'default': {
            'max_retries': 3, 'backoff': 'fixed', 'base_delay': 10,
            'jitter': 0.5,
        }})
        for _ in range(10):
            delay = policy.next_delay('network', 0)
            self.assertGreaterEqual(delay, 10)
            self.assertLessEqual(delay, 15)


if __name__ == '__main__':
    unittest.main()
