from connector.rest.rest_client_operation import RESTClientOperation
from core.setting import Setting


class GravitinoContext:
    def __init__(self, setting: Setting):
        self.metalake_name = setting.metalake_name
        self.uri = setting.uri
        self.gravitino_connector = RESTClientOperation(self.metalake_name, self.uri)

    def connector(self):
        return self.gravitino_connector
