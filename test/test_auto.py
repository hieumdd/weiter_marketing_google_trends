from .utils import process

GEO = "ML"


def test_auto():
    data = {
        "geo": GEO,
    }
    process(data)
