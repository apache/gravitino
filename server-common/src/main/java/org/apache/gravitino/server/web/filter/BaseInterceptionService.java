/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *  http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.gravitino.server.web.filter;

import java.lang.reflect.Constructor;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.aopalliance.intercept.ConstructorInterceptor;
import org.glassfish.hk2.api.Descriptor;
import org.glassfish.hk2.api.Filter;
import org.glassfish.hk2.api.InterceptionService;

/** BaseInterceptionService defines some common logic for InterceptionService. */
public abstract class BaseInterceptionService implements InterceptionService {

  @Override
  public List<ConstructorInterceptor> getConstructorInterceptors(Constructor<?> constructor) {
    return Collections.emptyList();
  }

  public static class ClassListFilter implements Filter {
    private final Set<String> targetClasses;

    public ClassListFilter(Set<String> targetClasses) {
      this.targetClasses = new HashSet<>(targetClasses);
    }

    @Override
    public boolean matches(Descriptor descriptor) {
      String implementation = descriptor.getImplementation();
      return targetClasses.contains(implementation);
    }
  }
}
