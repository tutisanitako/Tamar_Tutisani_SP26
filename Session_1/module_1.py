from typing import List


def task_1(array: List[int], target: int) -> List[int]:
    # Find a pair of numbers in the array that sum to target.

    seen = {}
    for num in array:
        complement = target - num
        if seen.get(complement) is not None:
            return [complement, num]
        seen[num] = True
    return []


def task_2(number: int) -> int:
    # Reverse the digits of an integer.

    negative = number < 0
    number = abs(number)

    reversed_num = 0
    while number > 0:
        digit = number % 10
        reversed_num = reversed_num * 10 + digit
        number //= 10

    return -reversed_num if negative else reversed_num


def task_3(array: List[int]) -> int:
    # Find the first integer that appears more than once (left to right).

    for i in range(len(array)):
        idx = abs(array[i]) - 1
        if idx < 0 or idx >= len(array):
            continue
        if array[idx] < 0:
            return abs(array[i])
        array[idx] = -array[idx]
    return -1


def task_4(string: str) -> int:
    # Convert a Roman numeral string to an integer.

    roman_values = {
        'I': 1,
        'V': 5,
        'X': 10,
        'L': 50,
        'C': 100,
        'D': 500,
        'M': 1000,
    }

    result = 0
    prev = 0
    for char in reversed(string):
        curr = roman_values[char]
        if curr < prev:
            result -= curr
        else:
            result += curr
        prev = curr
    return result


def task_5(array: List[int]) -> int:
    # Find the minimum value in the array.

    smallest = array[0]
    for num in array[1:]:
        if num < smallest:
            smallest = num
    return smallest