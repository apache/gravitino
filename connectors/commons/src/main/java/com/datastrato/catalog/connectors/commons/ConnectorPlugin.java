package com.datastrato.catalog.connectors.commons;

/**
 * Plugin interface implemented by Connector.
 */
public interface ConnectorPlugin {
    /**
     * Returns the type of the plugin.
     *
     * @return Returns the type of the plugin.
     */
    String getType();
}
