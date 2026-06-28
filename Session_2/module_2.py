# from collections import defaultdict as dd
# from itertools import product
from typing import Any, Dict, List, Tuple


def task_1(data_1: Dict[str, int], data_2: Dict[str, int]):
    # Merge two dicts, adding values together for keys that appear in both.
    # Start with a copy of data_1, then go through data_2 and either
    # add to the existing value or insert the new key.
    result = dict(data_1)
    for key, value in data_2.items():
        if result.get(key) is not None:
            result[key] += value
        else:
            result[key] = value
    return result


def task_2():
    # Build a dict of {number: number squared} for 1 through 15.
    # Dict comprehension keeps it clean and readable.
    return {i: i ** 2 for i in range(1, 16)}


def task_3(data: Dict[Any, List[str]]):
    # Generate all combinations by picking one letter from each key's list.
    # Start with a list containing an empty string, then for each key's options
    # extend every existing combination with each new letter.
    combinations = [""]
    for options in data.values():
        combinations = [existing + letter for existing in combinations for letter in options]
    return combinations


def task_4(data: Dict[str, int]):
    # Sort keys by their values descending and return the top 3.
    # If fewer than 3 keys exist, return however many there are.
    sorted_keys = sorted(data, key=lambda k: data[k], reverse=True)
    return sorted_keys[:3]


def task_5(data: List[Tuple[Any, Any]]) -> Dict[str, List[int]]:
    # Group values by key into lists.
    # For each (key, value) pair, append the value to the matching list in the dict.
    result = {}
    for key, value in data:
        if result.get(key) is None:
            result[key] = []
        result[key].append(value)
    return result


def task_6(data: List[Any]):
    # Remove duplicates while preserving order and respecting type differences.
    # "2" and 2 are treated as different elements, so we can't just use a set.
    # Track seen elements as (value, type) pairs to handle that correctly.
    seen = []
    result = []
    for item in data:
        identifier = (item, type(item))
        if identifier not in seen:
            seen.append(identifier)
            result.append(item)
    return result


def task_7(words: List[str]) -> str:
    # zip(*words) groups characters by position across all strings and stops
    # at the shortest word automatically, so no index errors.
    # If all characters at a position are the same, add to prefix, otherwise stop.
    prefix = ""
    for chars in zip(*words):
        if len(set(chars)) == 1:
            prefix += chars[0]
        else:
            break
    return prefix


def task_8(haystack: str, needle: str) -> int:
    # Return the index of the first occurrence of needle in haystack.
    # Empty needle always returns 0 per the task spec.
    # Manual character-by-character search without using built-in find/index.
    if needle == "":
        return 0
    if len(needle) > len(haystack):
        return -1
    for i in range(len(haystack) - len(needle) + 1):
        match = True
        for j in range(len(needle)):
            if haystack[i + j] != needle[j]:
                match = False
                break
        if match:
            return i
    return -1