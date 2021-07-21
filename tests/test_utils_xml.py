import os
import sys


sys.path.insert(1, os.path.join(sys.path[0], ".."))
from rich import traceback
from mabel.utils import xml_parse

traceback.install()


T = """<mydocument has="an attribute">
  <and>
    <many>elements</many>
    <many>more elements</many>
  </and>
  <plus a="complex">
    element as well
  </plus>
</mydocument>"""

def test_xml_parse():
  doc = xml_parse.parse(T)
  print(doc)
  assert doc['mydocument']['@has'] == u'an attribute'
  assert doc['mydocument']['and']['many'] == [u'elements', u'more elements']
  assert doc['mydocument']['plus']['@a'] == u'complex'
  assert doc['mydocument']['plus']['#text'] == u'element as well'

if __name__ == "__main__":
  test_xml_parse()