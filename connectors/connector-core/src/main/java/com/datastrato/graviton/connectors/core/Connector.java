package com.datastrato.graviton.connectors.core;

import java.io.Closeable;

public interface Connector extends Closeable
{
    @Override
    void close();
}
