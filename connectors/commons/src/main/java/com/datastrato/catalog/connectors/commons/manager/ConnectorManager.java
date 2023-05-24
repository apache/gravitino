package com.datastrato.catalog.connectors.commons.manager;

import com.datastrato.catalog.connectors.commons.*;
import com.google.common.base.Preconditions;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Connector manager.
 */
@Slf4j
public class ConnectorManager {
    private final ConcurrentMap<String, ConnectorPlugin> plugins = new ConcurrentHashMap<>();
    private final AtomicBoolean stopped = new AtomicBoolean();

    public void stop() {
        if (stopped.getAndSet(true)) {
            return;
        }
    }

    public void addPlugin(final ConnectorPlugin connectorPlugin) {
        plugins.put(connectorPlugin.getType(), connectorPlugin);
    }

    public ConnectorPlugin getPlugin(final String connectorType) {
        Preconditions.checkNotNull(connectorType, "connectorType is null");
        final ConnectorPlugin result = plugins.get(connectorType);
        Preconditions.checkNotNull(result, "No connector plugin exists for type %s", connectorType);
        return result;
    }
}
