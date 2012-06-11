# -*- coding: utf-8 -*-

def word_count(key, value):
    """
    count number of words
    """
    for word in value.split():
        yield word, 1
