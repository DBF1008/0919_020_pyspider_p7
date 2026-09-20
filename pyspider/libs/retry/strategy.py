#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# vim: set et sw=4 ts=4 sts=4 ff=unix fenc=utf8:

"""
Retry delay strategies.

A strategy maps the zero-based retry counter ``retried`` to the delay in
seconds before the next attempt.
"""

import numbers


class RetryStrategy(object):
    """Abstract retry delay strategy."""

    def get_delay(self, retried):
        raise NotImplementedError

    def __call__(self, retried):
        return self.get_delay(retried)


class FixedDelay(RetryStrategy):
    """Constant delay between attempts."""

    def __init__(self, delay):
        self.delay = max(0, float(delay))

    def get_delay(self, retried):
        return self.delay


class ExponentialBackoff(RetryStrategy):
    """
    Exponential backoff with optional jitter.

    delay = min(max_delay, base_delay * multiplier ** retried)
    """

    def __init__(self, base_delay=30, multiplier=2, max_delay=24 * 60 * 60,
                 jitter=0):
        self.base_delay = float(base_delay)
        self.multiplier = float(multiplier)
        self.max_delay = float(max_delay)
        self.jitter = float(jitter)

    def get_delay(self, retried):
        import random
        delay = min(
            self.max_delay,
            self.base_delay * self.multiplier ** max(0, int(retried))
        )
        if self.jitter:
            delay += random.random() * self.jitter
        return max(0, delay)


class CustomDelay(RetryStrategy):
    """
    Delay defined by an explicit mapping ``{retried: seconds}``.

    Falls back to ``default_delay`` for attempts beyond the table.
    """

    def __init__(self, delays=None, default_delay=24 * 60 * 60):
        self.delays = dict(delays or {})
        self.default_delay = float(default_delay)

    def get_delay(self, retried):
        if retried in self.delays:
            return float(self.delays[retried])
        if '' in self.delays:
            return float(self.delays[''])
        return self.default_delay


def build_strategy(config):
    """
    Build a strategy from configuration.

    Accepted forms::

        30                      -> FixedDelay(30)
        {'': 30, 1: 60}         -> CustomDelay (legacy retry_delay table)
        {'type': 'fixed', 'delay': 30}
        {'type': 'exponential', 'base_delay': 30, 'multiplier': 2,
         'max_delay': 86400, 'jitter': 0}
        {'type': 'custom', 'delays': {0: 10, 1: 60}, 'default_delay': 3600}
        RetryStrategy instance  -> returned as-is
    """
    if isinstance(config, RetryStrategy):
        return config

    if config is None:
        return ExponentialBackoff()

    if isinstance(config, numbers.Number):
        return FixedDelay(config)

    if isinstance(config, dict):
        if 'type' in config:
            strategy_type = str(config['type']).lower()
            params = dict(config)
            params.pop('type')
            if strategy_type in ('fixed', 'constant'):
                return FixedDelay(**params)
            if strategy_type in ('exponential', 'backoff', 'exp'):
                return ExponentialBackoff(**params)
            if strategy_type in ('custom', 'table', 'manual'):
                return CustomDelay(**params)
            raise ValueError('unknown retry strategy type: %r'
                             % config['type'])

        # legacy explicit retry-delay table: {0: 30, 1: 3600, '': 86400}
        table = {}
        for key, value in config.items():
            if key == '':
                table[''] = value
                continue
            try:
                table[int(key)] = value
            except (TypeError, ValueError):
                # leave non-index keys out; they cannot describe a delay row
                continue
        return CustomDelay(table, default_delay=table.get('', 86400))

    if callable(config):
        class _CallableStrategy(RetryStrategy):
            def get_delay(self, retried):
                return float(config(retried))
        return _CallableStrategy()

    raise ValueError('unsupported retry strategy config: %r' % (config,))
