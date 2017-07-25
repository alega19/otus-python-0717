#!/usr/bin/env python
# -*- coding: utf-8 -*-

from functools import wraps, update_wrapper


def disable(func):
    '''
    Disable a decorator by re-assigning the decorator's name
    to this function. For example, to turn off memoization:

    >>> memo = disable

    '''
    return func


def decorator(func):
    '''
    Decorate a decorator so that it inherits the docstrings
    and stuff from the function it's decorating.
    '''
    @wraps(func)
    def wrapper(*args, **kwargs):
        return func(*args, **kwargs)

    return wrapper


def countcalls(func):
    '''Decorator that counts calls made to the function decorated.'''

    @wraps(func)
    def wrapper(*args, **kwargs):
        wrapper.calls = getattr(wrapper, 'calls', 0) + 1
        return func(*args, **kwargs)

    return wrapper


def memo(func):
    '''
    Memoize a function so that it caches all return values for
    faster future lookups.
    '''

    @wraps(func)
    def wrapper(*args, **kwargs):
        update_wrapper(wrapper, func)
        key = args
        if key in wrapper._cache:
            return wrapper._cache[key]
        result = func(*args, **kwargs)
        wrapper._cache[key] = result
        return result

    wrapper._cache = {}

    return wrapper


def n_ary(func):
    '''
    Given binary function f(x, y), return an n_ary function such
    that f(x, y, z) = f(x, f(y,z)), etc. Also allow f(x) = x.
    '''

    @wraps(func)
    def wrapper(*args, **kwargs):
        if len(args) == 1:
            return args[0]
        elif len(args) == 2:
            return func(args[0], args[1], **kwargs)
        else:
            args = args[:-2] + (func(*args[-2:]),)
            return wrapper(*args, **kwargs)

    return wrapper


def trace(line):
    '''Trace calls made to function decorated.

    @trace("____")
    def fib(n):
        ....

    >>> fib(3)
     --> fib(3)
    ____ --> fib(2)
    ________ --> fib(1)
    ________ <-- fib(1) == 1
    ________ --> fib(0)
    ________ <-- fib(0) == 1
    ____ <-- fib(2) == 2
    ____ --> fib(1)
    ____ <-- fib(1) == 1
     <-- fib(3) == 3

    '''

    def dec(func):

        @wraps(func)
        def wrapper(*args, **kwargs):
            update_wrapper(wrapper, func)
            args_str = '('+str(args[0])+')' if len(args) == 1 else str(args)
            print line * wrapper._depth, '-->', func.__name__ + args_str
            wrapper._depth += 1
            result = func(*args, **kwargs)
            wrapper._depth -= 1
            print line * wrapper._depth, '<--', func.__name__ + args_str, '==', result
            return result

        wrapper._depth = 0

        return wrapper

    return dec


@memo
@countcalls
@n_ary
def foo(a, b):
    return a + b


@countcalls
@memo
@n_ary
def bar(a, b):
    return a * b


@trace("####")
@memo
@countcalls
def fib(n):
    return 1 if n <= 1 else fib(n-1) + fib(n-2)


def main():
    print foo(4, 3)
    print foo(4, 3, 2)
    print foo(4, 3)
    print "foo was called", foo.calls, "times"

    print bar(4, 3)
    print bar(4, 3, 2)
    print bar(4, 3, 2, 1)
    print "bar was called", bar.calls, "times"

    print fib.__doc__
    fib(13)
    print fib.calls, 'calls made'


if __name__ == '__main__':
    main()
