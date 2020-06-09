#! /usr/bin/env python3
from sys import argv

fname = argv[1]

from botocore.utils import calculate_tree_hash
with open(fname, "rb") as f:
    print(calculate_tree_hash(f))

