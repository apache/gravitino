/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.integration.test.trino;

import com.datastrato.gravitino.common.test.util.CloseableGroup;
import java.io.Closeable;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class CloseableGroupTest {
  @Test
  public void callCloseableTest() throws Exception {
    Closeable closeable1 = Mockito.mock(Closeable.class);
    Closeable closeable2 = Mockito.mock(Closeable.class);
    Closeable closeable3 = Mockito.mock(Closeable.class);

    CloseableGroup closeableGroup = CloseableGroup.create();
    closeableGroup.register(closeable1);
    closeableGroup.register(closeable2);
    closeableGroup.register(closeable3);

    closeableGroup.close();
    Mockito.verify(closeable1).close();
    Mockito.verify(closeable2).close();
    Mockito.verify(closeable3).close();
  }

  @Test
  public void callAutoCloseableTest() throws Exception {
    Closeable closeable1 = Mockito.mock(Closeable.class);
    AutoCloseable closeable2 = Mockito.mock(AutoCloseable.class);

    CloseableGroup closeableGroup = CloseableGroup.create();
    closeableGroup.register(closeable1);
    closeableGroup.register(closeable2);

    closeableGroup.close();
    Mockito.verify(closeable1).close();
    Mockito.verify(closeable2).close();
  }
}
