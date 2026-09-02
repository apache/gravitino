/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.gravitino.audit;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestCallerContext {
  @Test
  public void testCallerContext() {
    try {
      Map<String, String> contextMap = new HashMap<>();
      contextMap.put("test", "test");
      contextMap.put("test2", "test2");
      CallerContext callerContext = CallerContext.builder().withContext(contextMap).build();
      CallerContext.CallerContextHolder.set(callerContext);

      CallerContext actualCallerContext = CallerContext.CallerContextHolder.get();

      Assertions.assertEquals(callerContext, actualCallerContext);
      Assertions.assertEquals(callerContext.context(), actualCallerContext.context());
    } finally {
      CallerContext.CallerContextHolder.remove();
    }
  }

  @Test
  public void testContextIsDefensivelyCopied() {
    Map<String, String> contextMap = new HashMap<>();
    contextMap.put("test", "test");
    CallerContext callerContext = CallerContext.builder().withContext(contextMap).build();

    // Mutating the original map after build must not affect the stored context.
    contextMap.put("added-after-build", "value");
    contextMap.remove("test");

    Assertions.assertEquals(1, callerContext.context().size());
    Assertions.assertEquals("test", callerContext.context().get("test"));
    Assertions.assertFalse(callerContext.context().containsKey("added-after-build"));
  }

  @Test
  public void testContextIsUnmodifiable() {
    Map<String, String> contextMap = new HashMap<>();
    contextMap.put("test", "test");
    CallerContext callerContext = CallerContext.builder().withContext(contextMap).build();

    Assertions.assertThrows(
        UnsupportedOperationException.class, () -> callerContext.context().put("k", "v"));
  }

  @Test
  public void testNullContextStillRejectedOnBuild() {
    CallerContext.Builder builder = CallerContext.builder().withContext(null);
    Assertions.assertThrows(IllegalArgumentException.class, builder::build);
  }

  @Test
  public void testEmptyContextIsAllowed() {
    CallerContext callerContext = CallerContext.builder().withContext(new HashMap<>()).build();
    Assertions.assertTrue(callerContext.context().isEmpty());
  }

  @Test
  public void testReusingBuilderDoesNotMutateAlreadyBuiltContext() {
    Map<String, String> first = new HashMap<>();
    first.put("k", "v1");
    CallerContext.Builder builder = CallerContext.builder().withContext(first);
    CallerContext built = builder.build();

    // Reusing the same builder to set a different context must not affect the already-built
    // instance, which is now an independent immutable value object.
    Map<String, String> second = new HashMap<>();
    second.put("k", "v2");
    builder.withContext(second);

    Assertions.assertEquals("v1", built.context().get("k"));
  }

  @Test
  public void testContextEqualsAcrossMutatedSourceMap() {
    Map<String, String> source = new HashMap<>();
    source.put("k", "v");
    CallerContext expected = CallerContext.builder().withContext(source).build();

    // Mutating the source after building must not change equality against a context built from the
    // original contents.
    source.put("k2", "v2");
    CallerContext actual =
        CallerContext.builder().withContext(Collections.singletonMap("k", "v")).build();

    Assertions.assertEquals(expected, actual);
    Assertions.assertEquals(expected.hashCode(), actual.hashCode());
  }
}
