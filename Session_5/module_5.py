import re
from collections import Counter
import os
from pathlib import Path
from random import choice
from random import seed
from typing import List, Union

import requests
from requests.exceptions import RequestException


S5_PATH = Path(os.path.realpath(__file__)).parent

PATH_TO_NAMES = S5_PATH / "names.txt"
PATH_TO_SURNAMES = S5_PATH / "last_names.txt"
PATH_TO_OUTPUT = S5_PATH / "sorted_names_and_surnames.txt"
PATH_TO_TEXT = S5_PATH / "random_text.txt"
PATH_TO_STOP_WORDS = S5_PATH / "stop_words.txt"


def task_1():
    seed(1)

    with open(PATH_TO_NAMES, encoding="utf-8") as f:
        names = sorted({name.strip().lower() for name in f if name.strip()})

    with open(PATH_TO_SURNAMES, encoding="utf-8") as f:
        surnames = [surname.strip().lower() for surname in f if surname.strip()]

    with open(PATH_TO_OUTPUT, "w", encoding="utf-8") as out:
        for name in names:
            out.write(f"{name} {choice(surnames)}\n")


def task_2(top_k: int):
    with open(PATH_TO_STOP_WORDS, encoding="utf-8") as f:
        stop_words = {word.strip().lower() for word in f if word.strip()}

    with open(PATH_TO_TEXT, encoding="utf-8") as f:
        text = f.read().lower()

    # Extract purely alphabetic sequences, then drop stop words
    words = [w for w in re.findall(r"[a-z]+", text) if w not in stop_words]

    return Counter(words).most_common(top_k)


def task_3(url: str):
    headers = {"User-Agent": "Mozilla/5.0"}
    response = requests.get(url, headers=headers)
    # raise_for_status raises HTTPError (a subclass of RequestException) on 4xx/5xx
    try:
        response.raise_for_status()
    except Exception as e:
        raise RequestException(str(e)) from e
    return response


def task_4(data: List[Union[int, str, float]]):
    total = 0
    for item in data:
        try:
            total += item
        except TypeError:
            # item is a string — convert to float before adding
            total += float(item)
    return total


def task_5():
    a, b = input().split()
    try:
        result = float(a) / float(b)
    except ZeroDivisionError:
        print("Can't divide by zero")
        return
    except ValueError:
        print("Entered value is wrong")
        return

    # Print without unnecessary trailing decimal places
    if result == int(result):
        print(int(result))
    else:
        print(round(result, 3))