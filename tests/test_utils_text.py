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
    assert text.tokenize(QUOTE) == ['and', 'angels', 'are', 'away', 'but', 'devils', 'dying', 'earth', 'eckhart', 'freeing', 'frightened', 'from', 'hochheim', 'holding', 'if', 'life', 'made', 'of', 'on', 'peace', 'really', 'see', 'tearing', 'the', 'then', 'von', 'you', 'youll', 'your', 'youre', 'youve']
    assert text.tokenize(SNIPPET) == ['b', 'blink', 'center', 'html', 'important', 'is', 'this']


if __name__ == "__main__":  # pragma: no cover
    test_tokenizer()

    print("okay")
