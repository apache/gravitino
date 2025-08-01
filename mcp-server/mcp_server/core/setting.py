class Setting:
    def __init__(self, metalake_name: str, uri: str, mode: str):
        self.metalake_name = metalake_name
        self.uri = uri
        self.mode = mode

    def metalake(self):
        return self.metalake_name

    def uri(self):
        return self.uri

    def mode(self):
        return self.mode
