#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# vim: set et sw=4 ts=4 sts=4 ff=unix fenc=utf8:

"""
Unified error taxonomy and classification for pyspider.

Error categories
----------------
NETWORK   - transport / proxy / DNS / timeout / remote server (5xx)
PARSE     - response decoding / JSON or HTML parsing failures
BUSINESS  - user callback logic / HTTP 4xx / business-rule failures
SYSTEM    - internal defects (missing project, bad task, unknown error)

Handlers raise :class:`TaskError` (or subclasses) to force a specific
category.  Every category carries an independent retry policy (see
:mod:`pyspider.libs.retry.policy`).
"""

import re


class ErrorType(object):
    """Canonical error category names."""
    NETWORK = 'network'
    PARSE = 'parse'
    BUSINESS = 'business'
    SYSTEM = 'system'
    UNKNOWN = 'unknown'

    ALL = (NETWORK, PARSE, BUSINESS, SYSTEM, UNKNOWN)


_PARSE_EXCEPTION_MARKERS = (
    'json', 'decode', 'parse', 'parser', 'lookup', 'unicode', 'syntax',
    'attribute', 'key', 'index', 'type',
)
_PARSE_MESSAGE_MARKERS = ('json', 'decode', 'pars', 'encoding', 'xpath',
                          'selector', 'html', 'xml', 'csv')
_NETWORK_EXCEPTION_MARKERS = (
    'timeout', 'connection', 'socket', 'dns', 'ssl', 'curl', 'resolver',
    'reset', 'refused', 'unreachable', 'brokenpipe', 'incomplete',
)
_SYSTEM_EXCEPTION_MARKERS = (
    'assert', 'keyboardinterrupt', 'memory', 'systemexit', 'notimplemented',
)


class TaskError(Exception):
    """Base class for errors with an explicit :attr:`error_type`."""

    error_type = ErrorType.UNKNOWN

    def __init__(self, message='', error_type=None):
        Exception.__init__(self, message)
        if error_type is not None:
            self.error_type = error_type


class NetworkError(TaskError):
    error_type = ErrorType.NETWORK


class ParseError(TaskError):
    error_type = ErrorType.PARSE


class BusinessError(TaskError):
    error_type = ErrorType.BUSINESS


class SystemError(TaskError):
    error_type = ErrorType.SYSTEM


def _exception_names(error):
    """Return the lowercased class name and its trailing-error-stripped form."""
    name = type(error).__name__.lower()
    stripped = name[:-5] if name.endswith('error') else name
    return name, stripped


def classify_exception(error):
    """Classify an exception instance (or class name) into an error type."""
    if error is None:
        return ErrorType.UNKNOWN

    if isinstance(error, TaskError):
        return error.error_type

    explicit = getattr(error, 'error_type', None)
    if isinstance(explicit, str):
        return explicit

    name, stripped = _exception_names(error)
    if not name:
        return ErrorType.UNKNOWN

    for needle in _SYSTEM_EXCEPTION_MARKERS:
        if needle in name:
            return ErrorType.SYSTEM

    if 'json' in name:
        return ErrorType.PARSE

    for needle in _PARSE_EXCEPTION_MARKERS:
        if needle in name or needle in stripped:
            return ErrorType.PARSE

    for needle in _NETWORK_EXCEPTION_MARKERS:
        if needle in name or needle in stripped:
            return ErrorType.NETWORK

    # ValueError('bad json') and friends: fall back to the message text.
    if stripped == 'value':
        message = str(error).lower()
        if any(needle in message for needle in _PARSE_MESSAGE_MARKERS):
            return ErrorType.PARSE

    return ErrorType.BUSINESS


_NETWORK_STATUS_CODES = (599,) + tuple(range(500, 600))
_PARSE_STATUS_CODES = (501,)
_BUSINESS_STATUS_CODES = tuple(range(400, 500))


def classify_status_code(status_code):
    """
    Classify an HTTP status code.

    4xx -> BUSINESS (bad request / auth / not found ...)
    5xx / 599 -> NETWORK (remote server / transport failure)
    2xx / 3xx / other -> None (not an error at this level)
    """
    try:
        status_code = int(status_code)
    except (TypeError, ValueError):
        return ErrorType.UNKNOWN

    if status_code in _PARSE_STATUS_CODES:
        return ErrorType.PARSE
    if status_code in _NETWORK_STATUS_CODES:
        return ErrorType.NETWORK
    if status_code in _BUSINESS_STATUS_CODES:
        return ErrorType.BUSINESS
    return None


def classify_error_message(message):
    """Best-effort category from a free-form error string."""
    if not message:
        return ErrorType.UNKNOWN

    text = message.lower()
    for needle in _NETWORK_EXCEPTION_MARKERS:
        if needle in text:
            return ErrorType.NETWORK

    if re.search(r'\b(json|parse|decode|encoding|value error)\b', text):
        return ErrorType.PARSE

    return ErrorType.UNKNOWN


def classify_fetch_error(error=None, status_code=None):
    """
    Classify a fetcher error.

    ``error`` may be an exception instance or an error message string.
    Explicit exception categories win; otherwise the HTTP status code is
    used; the error message is consulted when no status code is available.
    """
    if error is not None and not isinstance(error, str):
        exc_type = classify_exception(error)
        if exc_type not in (ErrorType.UNKNOWN, ErrorType.BUSINESS):
            return exc_type

    if isinstance(error, str):
        msg_type = classify_error_message(error)
        if msg_type != ErrorType.UNKNOWN:
            return msg_type

    status_type = classify_status_code(status_code)
    if status_type not in (None, ErrorType.UNKNOWN):
        return status_type

    if error is None:
        return ErrorType.UNKNOWN
    return ErrorType.NETWORK


def classify_task_error(task):
    """
    Classify the error carried by a scheduler status pack.

    The fetcher and processor stamp ``error_type`` into track sections;
    this function reads those stamps and falls back to deriving the
    category from the process exception/logs, the fetch status code and
    error message, in that order.
    """
    track = (task or {}).get('track') or {}
    fetch = track.get('fetch') or {}
    process = track.get('process') or {}

    process_ok = process.get('ok')
    fetch_ok = fetch.get('ok')

    if not fetch and not process:
        return ErrorType.UNKNOWN

    # explicit stamps win (process over fetch)
    error_type = process.get('error_type') or fetch.get('error_type')
    if error_type in ErrorType.ALL:
        return error_type

    if not process_ok:
        exception = process.get('exception')
        if isinstance(exception, BaseException):
            exc_type = classify_exception(exception)
            if exc_type != ErrorType.UNKNOWN:
                return exc_type
        elif isinstance(exception, str) and exception:
            msg_type = classify_error_message(exception)
            if msg_type != ErrorType.UNKNOWN:
                return msg_type

        message = process.get('logs') or ''
        if isinstance(message, str) and message:
            if 'Traceback' in message:
                return ErrorType.BUSINESS
            msg_type = classify_error_message(message)
            if msg_type != ErrorType.UNKNOWN:
                return msg_type

    if not fetch_ok:
        fetch_type = classify_fetch_error(
            error=fetch.get('error'),
            status_code=fetch.get('status_code'),
        )
        if fetch_type != ErrorType.UNKNOWN:
            return fetch_type
        return ErrorType.NETWORK

    if not process_ok:
        return ErrorType.BUSINESS

    return ErrorType.UNKNOWN
