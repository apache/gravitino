package com.datastrato.catalog.connectors.commons;

/**
 * Factory that returns the connector implementations of the service.
 */
public interface ConnectorFactory {
    String UNSUPPORTED_MESSAGE = "Not supported by this connector";

    /**
     * Shuts down the factory.
     */
    void stop();
}
