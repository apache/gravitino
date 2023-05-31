package com.datastrato.graviton.connectors.core;

import java.util.Map;

public interface Connector
{
    String getName();

//    Connector create(String catalogName, Map<String, String> config, ConnectorContext context);
}
