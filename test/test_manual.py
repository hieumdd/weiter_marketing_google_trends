from .utils import process


START = "2021-08-01"
END = "2021-08-18"


def test_top_campaign_content():
    data = {
        "start": START,
        "end": END,
    }
    process(data)
