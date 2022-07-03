import numpy
import pprint

nums = [1, 2, 3, 4, 5]


def chunks(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

pprint.pprint(list(chunks(nums, 3)))
