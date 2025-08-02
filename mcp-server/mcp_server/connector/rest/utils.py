import json

def extract_content_from_response(response, field: str, default="") -> str:
    response.raise_for_status()
    return json.dumps(response.json().get(field, default))
