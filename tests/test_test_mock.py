from helpers import Mock
from rich import traceback

traceback.install()


def test_mock():
    mock = Mock({"name": "I'm a Mock", "child": Mock({"name": "My parent is a Mock"})}, ["name"])

    assert mock.name == "I'm a Mock"
    assert mock.rank() == "rank"
    assert isinstance(mock.child(), Mock)
    assert mock.child().name() == "My parent is a Mock"


if __name__ == "__main__":  # pragma: no cover
    from helpers.runner import run_tests

    run_tests()
