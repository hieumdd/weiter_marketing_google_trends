from .utils import process


START = "2020-08-01"
END = "2021-08-18"


def test_manual():
    data = {
        "start": START,
        "end": END,
    }
    process(data)
