package com.datastrato.unified_catalog.connector.mysql;

import com.datastrato.unified_catalog.connectors.commons.ConnectorPluginManager;
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
        ConnectorPluginManager pluginManager = new ConnectorPluginManager();

        pluginManager.loadPlugins();
        assert (pluginManager.getPlugin(MySqlConnectorPlugin.CONNECTOR_TYPE) instanceof MySqlConnectorPlugin);
    }
}
