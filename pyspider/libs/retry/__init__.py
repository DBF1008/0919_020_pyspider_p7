#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# vim: set et sw=4 ts=4 sts=4 ff=unix fenc=utf8:

"""Pluggable retry strategies and per-error-type retry policies."""

from .strategy import (
    RetryStrategy, FixedDelay, ExponentialBackoff, CustomDelay, build_strategy,
)
from .policy import RetryPolicy, build_policy, DEFAULT_RETRY_POLICY, LEGACY_RETRY_DELAY

__all__ = [
    'RetryStrategy', 'FixedDelay', 'ExponentialBackoff', 'CustomDelay',
    'build_strategy', 'RetryPolicy', 'build_policy', 'DEFAULT_RETRY_POLICY',
    'LEGACY_RETRY_DELAY',
]
