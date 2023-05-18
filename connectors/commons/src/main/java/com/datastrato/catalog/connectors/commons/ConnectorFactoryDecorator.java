package com.datastrato.catalog.connectors.commons;

import lombok.Getter;
import lombok.NonNull;

/**
 * A decorator for a connector factory to add additional functionality
 * to all connector services.
 */
public class ConnectorFactoryDecorator implements ConnectorFactory {
    private final ConnectorPlugin connectorPlugin;
    @Getter
    private final ConnectorFactory factory;

    /**
     * Creates the decorated connector factory that wraps connector services
     * with additional wrappers.
     *
     * @param connectorPlugin the underlying plugin
     */
    public ConnectorFactoryDecorator(@NonNull final ConnectorPlugin connectorPlugin) {
        this.connectorPlugin = connectorPlugin;
        this.factory = connectorPlugin.create();
    }

    @Override
    public void stop() {
        factory.stop();
    }

}
