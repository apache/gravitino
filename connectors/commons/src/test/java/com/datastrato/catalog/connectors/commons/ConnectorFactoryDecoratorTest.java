package com.datastrato.catalog.connectors.commons;

import org.junit.Test;
import org.mockito.Mockito;

import static org.mockito.Mockito.mock;

public class ConnectorFactoryDecoratorTest {
  private ConnectorFactory factory;
  private ConnectorPlugin connectorPlugin;

  @Test
  public void testInsert() throws Exception {
    factory = mock(ConnectorFactory.class);
    connectorPlugin = mock(ConnectorPlugin.class);

    Mockito.when(connectorPlugin.create()).thenReturn(factory);
  }
}
