package com.datastrato.catalog.connectors.commons;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Plugin Manager. This loads the connector plugins using the ServiceLoader.
 * Connector plugins need to be loaded before loading the catalogs.
 */
@Slf4j
public class ConnectorPluginManager {
    // Map<ConnectorType, ConnectorPlugin>
    private final ConcurrentMap<String, ConnectorPlugin> plugins = new ConcurrentHashMap<>();

    private final AtomicBoolean pluginsLoading = new AtomicBoolean();

    /**
     * Loads the plugins.
     */
    public void loadPlugins() throws Exception {
        if (!this.pluginsLoading.compareAndSet(false, true)) {
            return;
        }

        final ServiceLoader<ConnectorPlugin> serviceLoader =
            ServiceLoader.load(ConnectorPlugin.class, this.getClass().getClassLoader());
        final List<ConnectorPlugin> connectorPlugins = ImmutableList.copyOf(serviceLoader);

        if (connectorPlugins.isEmpty()) {
            log.warn("No connector plugin of type {}", ConnectorPlugin.class.getName());
        }

        for (ConnectorPlugin connectorPlugin : connectorPlugins) {
            log.info("Installing {} plugin", connectorPlugin.getClass().getName());
            this.installPlugin(connectorPlugin);
            log.info("-- Finished loading plugin {} --", connectorPlugin.getClass().getName());
        }
    }

    /**
     * Installs the plugins.
     */
    private void installPlugin(final ConnectorPlugin connectorPlugin) {
        plugins.put(connectorPlugin.getType(), connectorPlugin);
    }

    public ConnectorPlugin getPlugin(final String connectorType) {
        Preconditions.checkNotNull(connectorType, "connectorType is null");
        final ConnectorPlugin result = plugins.get(connectorType);
        Preconditions.checkNotNull(result, "No connector plugin exists for type %s", connectorType);
        return result;
    }
}
