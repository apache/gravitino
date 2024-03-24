/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.storage.kv;

import com.datastrato.gravitino.Config;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.rocksdb.Options;

public class TestRocksDBOptions {
  @Test
  void testSetOptions() {
    String prefix = "gravitino.entity.store.kv.rocksdb";
    String optionsKey = prefix + ".Options.maxBackgroundJobs";

    Map<String, String> mockConfigMap = new HashMap<String, String>();
    // set maxBackgroundJobs to 8
    int expectMaxBackgroundJobs = 8;
    Config config = Mockito.mock(Config.class);
    mockConfigMap.put(optionsKey, String.valueOf(expectMaxBackgroundJobs));

    Mockito.when(config.getConfigsWithPrefix(prefix)).thenReturn(mockConfigMap);

    Options mockOptions = Mockito.spy(new Options());

    AtomicInteger maxBackgroundJobs = new AtomicInteger(0);
    // Mock the setMaxBackgroundJobs method
    Mockito.doAnswer(
            new Answer<Void>() {
              public Void answer(InvocationOnMock invocation) {
                Object[] args = invocation.getArguments();
                if (args.length > 0 && args[0] instanceof Integer) {
                  maxBackgroundJobs.set(expectMaxBackgroundJobs);
                }
                return null;
              }
            })
        .when(mockOptions)
        .setMaxBackgroundJobs(Mockito.anyInt());

    RocksDBOptions options = new RocksDBOptions(mockOptions, null, null);
    options.setOptions(config);

    Assertions.assertDoesNotThrow(() -> options.setOptions(config));
    Assertions.assertEquals(expectMaxBackgroundJobs, maxBackgroundJobs.get());
  }
}
