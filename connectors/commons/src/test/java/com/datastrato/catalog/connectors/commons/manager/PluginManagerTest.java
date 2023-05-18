package com.datastrato.catalog.connectors.commons.manager;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class PluginManagerTest {
    @BeforeEach
    public void setup() {
    }

    @Test
    public void testAddPlugin() throws Exception {
        ConnectorManager connectorManager = new ConnectorManager();
        PluginManager pluginManager = new PluginManager(connectorManager);

        pluginManager.loadPlugins();
    }
}
