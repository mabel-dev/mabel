"""
Tests for paths to ensure the split and join methods
of paths return the expected values for various
stimulus.
"""

import sys
import os

sys.path.insert(1, os.path.join(sys.path[0], ".."))
from mabel.utils import text
from rich import traceback

traceback.install()

QUOTE = """
If you're frightened of dying and you're holding on,
you'll see devils tearing your life away.
But if you've made your peace,
then the devils are really angels,
freeing you from the earth.
-Eckhart von Hochheim
"""

SNIPPET = """
<html>
 <center>
  <blink>
   <b>
    THIS IS IMPORTANT
   </b>
  </blink>
 <center>
</html>
"""


def test_tokenizer():
    assert text.tokenize("the") == ["the"]
    assert text.tokenize("CVE-2017-0144") == ["cve20170144"]
    assert text.tokenize(QUOTE) == [
        "if",
        "youre",
        "frightened",
        "of",
        "dying",
        "and",
        "youre",
        "holding",
        "on",
        "youll",
        "see",
        "devils",
        "tearing",
        "your",
        "life",
        "away",
        "but",
        "if",
        "youve",
        "made",
        "your",
        "peace",
        "then",
        "the",
        "devils",
        "are",
        "really",
        "angels",
        "freeing",
        "you",
        "from",
        "the",
        "earth",
        "eckhart",
        "von",
        "hochheim",
    ]
    assert text.tokenize(SNIPPET) == [
        "html",
        "center",
        "blink",
        "b",
        "this",
        "is",
        "important",
        "b",
        "blink",
        "center",
        "html",
    ]


if __name__ == "__main__":  # pragma: no cover
    from tests.helpers.runner import run_tests

    run_tests()
