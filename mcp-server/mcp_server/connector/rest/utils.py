def get_json_from_response(response):
    response.raise_for_status()
    return response.json()
