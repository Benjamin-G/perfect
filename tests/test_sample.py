# Need this to import files
import os
import sys

# getting the name of the directory
# where the file is present.
current = os.path.dirname(os.path.realpath(__file__))

# Getting the parent directory name
# where the current directory is present.
parent = os.path.dirname(current)

# adding the parent directory to
# the sys.path.
sys.path.append(parent)

from main import *


def addTwo(x):
    return x + 2


def test_answer():
    assert addTwo(3) == 5


def test_add_together():
    # To test an individual task, you can access the original function using .fn:
    assert add_together.fn(2, 3) == 5
