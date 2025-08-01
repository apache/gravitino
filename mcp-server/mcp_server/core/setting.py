class Setting:
    def __init__(self, metalake_name: str, uri: str, mode: str):
        self._metalake_name = metalake_name
        self._uri = uri
        self._mode = mode

    def metalake(self):
        return self._metalake_name

    def uri(self):
        return self._uri

    def mode(self):
        return self._mode
