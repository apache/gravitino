/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.graviton.utils;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestMapUtils {

  @Test
  public void testGetPrefixMap() {
    Map configs = ImmutableMap.of("a.b", "", "a.c", "", "", "", "b.a", "");

    Assertions.assertEquals(ImmutableMap.of(), MapUtils.getPrefixMap(configs, "xx"));
    Assertions.assertThrowsExactly(
        NullPointerException.class, () -> MapUtils.getPrefixMap(configs, null));
    Assertions.assertEquals(
        ImmutableMap.of("b", "", "c", ""), MapUtils.getPrefixMap(configs, "a."));
    Assertions.assertEquals(ImmutableMap.of("a", ""), MapUtils.getPrefixMap(configs, "b."));
    Assertions.assertEquals(configs, MapUtils.getPrefixMap(configs, ""));
  }
}
