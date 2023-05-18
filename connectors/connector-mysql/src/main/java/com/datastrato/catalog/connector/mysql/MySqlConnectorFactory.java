package com.datastrato.catalog.connector.mysql;

import com.datastrato.catalog.connectors.commons.DefaultConnectorFactory;
import com.google.common.collect.Lists;

/**
 * MySql implementation of a connector factory.
 */
class MySqlConnectorFactory extends DefaultConnectorFactory {

    /**
     * Constructor.
     */
    MySqlConnectorFactory() {
        super(Lists.newArrayList(new MySqlConnectorModule()));
    }
}
