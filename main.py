import json
import base64

from models import InterestOverTime
from broadcast import broadcast

def main(request):
    request_json = request.get_json(silent=True)
    message = request_json["message"]
    data_bytes = message["data"]
    data = json.loads(base64.b64decode(data_bytes).decode("utf-8"))
    print(data)

    if "broadcast" in data:
        results = broadcast()
    elif "geo" in data:
        job = InterestOverTime(
            geo=data["geo"],
        )
        results = job.run()
    else:
        raise NotImplementedError(data)

    responses = {
        "pipelines": "GoogleTrends",
        "results": results,
    }
    print(responses)
    return responses
