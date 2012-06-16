# -*- coding: utf-8 -*-
import os

def word_count_map(key, value):
    """
    "Mapper". Count number of words
    """
    for word in value.split():
        yield word, 1


def word_count_reduce(word, values):
    """
    "Reducer". Count number of words
    """
    yield word, sum(values)


def read_file(source):
    """
    "Source reader". Read contents of the file line by line

    Yield key, value in the form of (lineno, line)
    """
    if not os.path.isfile(source):
        return
    with open(source) as fd:
        for i, line in enumerate(fd.readlines()):
            yield i, line.strip()
