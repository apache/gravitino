from mcp_server.core import Setting
from mcp_server.connector import ConnectorFactory


class GravitinoContext:
    def __init__(self, setting: Setting):
        self.gravitino_connector = ConnectorFactory.create_connector(setting.metalake(), setting.uri())

    def connector(self):
        return self.gravitino_connector
