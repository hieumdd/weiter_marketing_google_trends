from .utils import process

GEO = "VN"


def test_auto():
    data = {
        "geo": GEO,
    }
    process(data)
