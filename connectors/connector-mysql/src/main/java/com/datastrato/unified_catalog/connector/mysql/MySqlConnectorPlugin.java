package com.datastrato.unified_catalog.connector.mysql;

import com.datastrato.unified_catalog.connectors.commons.ConnectorPlugin;
import com.google.auto.service.AutoService;
import com.google.common.annotations.VisibleForTesting;

/**
 * Implementation of the ConnectorPlugin interface for MySQL.
 */
@AutoService(ConnectorPlugin.class)
public class MySqlConnectorPlugin implements ConnectorPlugin {
    @VisibleForTesting
    protected static final String CONNECTOR_TYPE = "mysql";

    @Override
    public String getType() {
        return CONNECTOR_TYPE;
    }
}
