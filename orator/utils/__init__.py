# -*- coding: utf-8 -*-

import sys  # noqa
import imp  # noqa
import warnings  # noqa
import functools  # noqa
from functools import reduce  # noqa
from .helpers import mkdir_p, value  # noqa
from urllib.parse import (quote_plus, unquote_plus,  # noqa
                          parse_qsl, quote, unquote)  # noqa


def load_module(module, path):
    with open(path, 'rb') as fh:
        mod = imp.load_source(module, path, fh)
    return mod


class Null:

    def __bool__(self):
        return False

    def __eq__(self, other):
        return other is None


def deprecated(func):
    '''This is a decorator which can be used to mark functions
    as deprecated. It will result in a warning being emitted
    when the function is used.'''

    @functools.wraps(func)
    def new_func(*args, **kwargs):
        func_code = func.__code__

        warnings.warn_explicit(
            "Call to deprecated function {}.".format(func.__name__),
            category=DeprecationWarning,
            filename=func_code.co_filename,
            lineno=func_code.co_firstlineno + 1
        )

        return func(*args, **kwargs)

    return new_func


def decode(string, encodings=None):
    if not isinstance(string, bytes):
        return string

    if encodings is None:
        encodings = ['utf-8', 'latin1', 'ascii']

    for encoding in encodings:
        try:
            return string.decode(encoding)
        except (UnicodeEncodeError, UnicodeDecodeError):
            pass

    return string.decode(encodings[0], errors='ignore')


def encode(string, encodings=None):
    if isinstance(string, bytes):
        return string

    if encodings is None:
        encodings = ['utf-8', 'latin1', 'ascii']

    for encoding in encodings:
        try:
            return string.encode(encoding)
        except (UnicodeEncodeError, UnicodeDecodeError):
            pass

    return string.encode(encodings[0], errors='ignore')
