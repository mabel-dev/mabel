import datetime
import sys
import os
sys.path.insert(1, os.path.join(sys.path[0], '..'))
from mabel.data.formats.dictset import display
try:
    from rich import traceback
    traceback.install()
except ImportError:   # pragma: no cover
    pass


def test_to_html():
    ds = [
        {'key': 1, 'value': 'one', 'plus1': 2},
        {'key': 2, 'value': 'two', 'plus1': 3},
        {'key': 3, 'value': 'three', 'plus1': 4},
        {'key': 4, 'value': 'four', 'plus1': 5}
    ]
    html = display.html_table(ds)

    # are the headers there
    assert "<th>key<th>" in html
    assert "<th>value<th>" in html
    assert "<th>plus1<th>" in html

    # test for some of the values
    assert "<td>one<td>" in html
    assert "<td>1<td>" in html
    assert "<td>5<td>" in html


def test_to_ascii():
    ds = [
        {'key': 1, 'value': 'one', 'plus1': 2},
        {'key': 2, 'value': 'two', 'plus1': 3},
        {'key': 3, 'value': 'three', 'plus1': 4},
        {'key': 4, 'value': 'four', 'plus1': 5}
    ]
    axki = display.ascii_table(ds)

    print(axki)

    # are the headers there
    assert " key " in axki
    assert " value " in axki
    assert " plus1 " in axki

    # test for some of the values
    assert " one " in axki
    assert " 1 " in axki
    assert " 5 " in axki


def test_histograms():
    
    data = [1,2,2,3,3,3,4,4,4,4,5,5,5,5,5,6,6,6,6,6,6,7,7,7,7,7,7,7]

    hist = display.histogram(data, 3)
    assert hist == r"▃█▃"

    hist = display.histogram(data, 7)
    assert hist == r"▁▂▃▄▅▆█"  


if __name__ == "__main__":
    test_to_html()
    test_to_ascii()
    test_histograms()
    
    print('okay')
