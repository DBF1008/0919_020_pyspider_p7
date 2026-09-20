#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# vim: set et sw=4 ts=4 sts=4 ff=unix fenc=utf8:

"""Per-error-type retry policy configuration."""

from pyspider.libs.error_handling import ErrorType
from .strategy import (
    RetryStrategy, ExponentialBackoff, CustomDelay, build_strategy,
)


# Legacy pyspider fixed retry-delay table, kept verbatim for back-compat.
LEGACY_RETRY_DELAY = {
    0: 30,
    1: 1 * 60 * 60,
    2: 6 * 60 * 60,
    3: 12 * 60 * 60,
    '': 24 * 60 * 60,
}


class RetryPolicy(object):
    """
    Retry policy for a single error category.

    :param max_retries: attempts after the first one; 0 disables retry.
    :param strategy: anything accepted by :func:`build_strategy`.
    """

    def __init__(self, max_retries=3, strategy=None):
        self.max_retries = int(max_retries)
        self.strategy = build_strategy(strategy)

    def get_delay(self, retried):
        return self.strategy.get_delay(retried)

    def should_retry(self, retried):
        return int(retried) < self.max_retries

    def to_dict(self):
        if isinstance(self.strategy, CustomDelay):
            strategy = {
                'type': 'custom',
                'delays': dict(self.strategy.delays),
                'default_delay': self.strategy.default_delay,
            }
        elif isinstance(self.strategy, ExponentialBackoff):
            strategy = {
                'type': 'exponential',
                'base_delay': self.strategy.base_delay,
                'multiplier': self.strategy.multiplier,
                'max_delay': self.strategy.max_delay,
                'jitter': self.strategy.jitter,
            }
        else:
            strategy = {
                'type': 'fixed',
                'delay': self.strategy.get_delay(0),
            }
        return {
            'max_retries': self.max_retries,
            'strategy': strategy,
        }


# NETWORK failures are transient: keep the historical escalating schedule.
# PARSE errors may be caused by temporary bad/partial content: retry a few.
# BUSINESS/4xx errors are usually deterministic: few or no retries.
# SYSTEM errors are internal defects: no automatic retries.
# UNKNOWN gets a conservative middle ground.
DEFAULT_RETRY_POLICY = {
    ErrorType.NETWORK: RetryPolicy(3, CustomDelay(LEGACY_RETRY_DELAY)),
    ErrorType.PARSE: RetryPolicy(2, ExponentialBackoff(base_delay=60,
                                                       multiplier=5,
                                                       max_delay=1800)),
    ErrorType.BUSINESS: RetryPolicy(1, ExponentialBackoff(base_delay=300,
                                                          multiplier=2,
                                                          max_delay=600)),
    ErrorType.SYSTEM: RetryPolicy(0, ExponentialBackoff(base_delay=10,
                                                        multiplier=2,
                                                        max_delay=600)),
    ErrorType.UNKNOWN: RetryPolicy(2, ExponentialBackoff(base_delay=30,
                                                         multiplier=2,
                                                         max_delay=600)),
}


def _build_category_policy(category, config):
    if isinstance(config, RetryPolicy):
        return config

    if isinstance(config, (int, float, RetryStrategy, dict)) or config is None:
        if isinstance(config, dict) and 'max_retries' in config:
            return RetryPolicy(
                max_retries=config['max_retries'],
                strategy=config.get('strategy'),
            )
        return RetryPolicy(max_retries=3, strategy=config)

    raise ValueError('unsupported retry policy for %r: %r' % (category, config))


def build_policy(config=None, default_retries=3):
    """
    Build an ``{error_type: RetryPolicy}`` mapping from configuration.

    Accepted forms::

        None                              -> built-in defaults
        {'network': {...}, 'parse': 3, ...}
        {'default': {...}, 'network': {...}}
        {'retries': 3, 'strategy': {...}, 'network': {'max_retries': 5}}
        {0: 30, 1: 3600, '': 86400}       -> legacy retry_delay table
    """
    policies = {
        category: RetryPolicy(policy.max_retries, policy.strategy)
        for category, policy in DEFAULT_RETRY_POLICY.items()
    }

    if config is None:
        return policies

    if isinstance(config, dict) and config and all(
            key == '' or isinstance(key, int) or
            (isinstance(key, str) and key.isdigit())
            for key in config) and all(
            not isinstance(value, dict) for value in config.values()):
        # a plain retry-delay table (legacy project ``retry_delay`` shape)
        strategy = build_strategy(config)
        for category in policies:
            policies[category] = RetryPolicy(default_retries, strategy)
        return policies

    if not isinstance(config, dict):
        raise ValueError('unsupported retry policy config: %r' % (config,))

    shared_strategy = config.get('strategy')
    shared_retries = config.get('retries', default_retries)
    if shared_strategy is not None:
        for category in policies:
            policies[category] = RetryPolicy(shared_retries, shared_strategy)

    if 'default' in config:
        default_policy = _build_category_policy('default', config['default'])
        for category in policies:
            policies[category] = RetryPolicy(
                default_policy.max_retries, default_policy.strategy)

    for category in ErrorType.ALL:
        if category in config:
            policies[category] = _build_category_policy(category,
                                                        config[category])

    return policies
