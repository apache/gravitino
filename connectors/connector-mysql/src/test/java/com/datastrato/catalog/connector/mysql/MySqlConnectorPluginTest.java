package com.datastrato.catalog.connector.mysql;

import com.datastrato.catalog.connectors.commons.manager.ConnectorManager;
import com.datastrato.catalog.connectors.commons.manager.PluginManager;
import org.junit.jupiter.api.Test;
import java.util.Objects;

/**
 * Tests for the MySqlConnectorPlugin.
 */
class MySqlConnectorPluginTest {
    private final MySqlConnectorPlugin plugin = new MySqlConnectorPlugin();

    @Test
    public void getConnectorType() {
        assert (Objects.equals(plugin.getType(), "mysql"));
    }

    @Test
    public void loadMySqlPluginTest() throws Exception {
        ConnectorManager connectorManager = new ConnectorManager();
        PluginManager pluginManager = new PluginManager(connectorManager);

        pluginManager.loadPlugins();
        assert (connectorManager.getPlugin(MySqlConnectorPlugin.CONNECTOR_TYPE) instanceof MySqlConnectorPlugin);
    }
}
