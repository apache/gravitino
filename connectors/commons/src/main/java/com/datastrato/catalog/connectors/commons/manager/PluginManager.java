package com.datastrato.catalog.connectors.commons.manager;

import com.datastrato.catalog.connectors.commons.ConnectorPlugin;
import com.google.common.collect.ImmutableList;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.ServiceLoader;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Plugin Manager. This loads the connector plugins using the ServiceLoader.
 * Connector plugins need to be loaded before loading the catalogs.
 */
@Slf4j
public class PluginManager {
    private final ConnectorManager connectorManager;
    private final AtomicBoolean pluginsLoaded = new AtomicBoolean();
    private final AtomicBoolean pluginsLoading = new AtomicBoolean();

    public PluginManager(final ConnectorManager connectorManager) {
        this.connectorManager = connectorManager;
    }

    /**
     * Loads the plugins.
     *
     * @throws Exception error
     */
    public void loadPlugins() throws Exception {
        if (!this.pluginsLoading.compareAndSet(false, true)) {
            return;
        }

        final ServiceLoader<ConnectorPlugin> serviceLoader =
            ServiceLoader.load(ConnectorPlugin.class, this.getClass().getClassLoader());
        final List<ConnectorPlugin> connectorPlugins = ImmutableList.copyOf(serviceLoader);

        if (connectorPlugins.isEmpty()) {
            log.warn("No service providers of type {}", ConnectorPlugin.class.getName());
        }

        for (ConnectorPlugin connectorPlugin : connectorPlugins) {
            log.info("Installing {}", connectorPlugin.getClass().getName());
            this.installPlugin(connectorPlugin);
            log.info("-- Finished loading plugin {} --", connectorPlugin.getClass().getName());
        }

        this.pluginsLoaded.set(true);
    }

    /**
     * Installs the plugins.
     *
     * @param connectorPlugin service plugin
     */
    private void installPlugin(final ConnectorPlugin connectorPlugin) {
        this.connectorManager.addPlugin(connectorPlugin);
    }
}
