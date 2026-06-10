import time
from typing import List

Matrix = List[List[int]]


def task_1(exp: int):
    # Closure factory — the outer function captures `exp` in its enclosing scope.
    # Every call to power_factory returns a new closure with its own `exp` baked in.
    def power(base):
        return base ** exp
    return power


def task_2(*args, **kwargs):
    # *args captures positional arguments, **kwargs captures keyword arguments.
    # Printing them separately preserves the order they were passed in.
    for value in args:
        print(value)
    for value in kwargs.values():
        print(value)


def helper(func):
    # Decorator that wraps the function with greeting lines before and after.
    # functools.wraps isn't strictly needed here since the test calls by name directly.
    def wrapper(*args, **kwargs):
        print("Hi, friend! What's your name?")
        func(*args, **kwargs)
        print("See you soon!")
    return wrapper


@helper
def task_3(name: str):
    print(f"Hello! My name is {name}.")


def timer(func):
    # Measures and prints the runtime of the wrapped function.
    # time.perf_counter is preferred over time.time for short intervals.
    def wrapper(*args, **kwargs):
        start = time.perf_counter()
        result = func(*args, **kwargs)
        run_time = time.perf_counter() - start
        print(f"Finished {func.__name__} in {run_time:.4f} secs")
        return result
    return wrapper


@timer
def task_4():
    return len([1 for _ in range(0, 10**8)])


def task_5(matrix: Matrix) -> Matrix:
    # zip(*matrix) unpacks the rows and zips them by column position,
    # effectively swapping rows and columns. Works for any m x n matrix.
    return [list(row) for row in zip(*matrix)]


def task_6(queue: str):
    # Classic stack approach — push on '(' and pop on ')'.
    # If we ever try to pop from an empty stack, the string is invalid.
    # At the end, the stack must be empty for all brackets to be matched.
    stack = []
    for char in queue:
        if char == '(':
            stack.append(char)
        else:
            if not stack:
                return False
            stack.pop()
    return len(stack) == 0