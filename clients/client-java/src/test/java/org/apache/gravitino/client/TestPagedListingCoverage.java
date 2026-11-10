/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.gravitino.client;

import com.google.common.reflect.ClassPath;
import java.io.IOException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;
import org.apache.gravitino.pagination.PageRequest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Every paged list method on the public API is a default method that throws. Each concrete client
 * class must override every such method it inherits, so that no client call reaches the default.
 */
public class TestPagedListingCoverage {

  @Test
  public void testEveryClientClassImplementsInheritedPagedListing() throws IOException {
    List<String> missing = new ArrayList<>();
    int checked = 0;
    for (ClassPath.ClassInfo info :
        ClassPath.from(getClass().getClassLoader())
            .getTopLevelClasses(getClass().getPackage().getName())) {
      Class<?> cls = info.load();
      if (cls.isInterface()
          || Modifier.isAbstract(cls.getModifiers())
          || cls.getName().contains("Test")) {
        continue;
      }
      for (Method method : cls.getMethods()) {
        Class<?>[] params = method.getParameterTypes();
        if (params.length == 0 || params[params.length - 1] != PageRequest.class) {
          continue;
        }
        checked++;
        if (method.getDeclaringClass().isInterface()) {
          missing.add(cls.getSimpleName() + "#" + method.getName());
        }
      }
    }
    Assertions.assertTrue(checked > 0, "no paged methods found");
    Assertions.assertTrue(
        missing.isEmpty(), "client classes falling through to the unsupported default: " + missing);
  }
}
