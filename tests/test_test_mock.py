from helpers import Mock


def test_mock():

    mock = Mock({'name':'I\'m a Mock', 'child': Mock({'name': 'My parent is a Mock'})}, ['name'])

    assert mock.name == 'I\'m a Mock'
    assert mock.rank() == 'rank'
    assert isinstance(mock.child(), Mock)
    assert mock.child().name() == 'My parent is a Mock'


if __name__ == "__main__":
    test_mock()