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

import com.google.common.collect.ImmutableList;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.List;
import org.aopalliance.intercept.ConstructorInterceptor;
import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;
import org.glassfish.hk2.api.Filter;
import org.glassfish.hk2.api.InterceptionService;

/**
 * GravitinoInterceptionService defines a method interceptor for REST interfaces to create dynamic
 * proxies. It implements metadata authorization when invoking REST API methods. It needs to be
 * registered in the hk2 bean container when the gravitino server starts.
 */
public class GravitinoInterceptionService implements InterceptionService {

  @Override
  public Filter getDescriptorFilter() {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<MethodInterceptor> getMethodInterceptors(Method method) {
    return ImmutableList.of(new MetadataAuthorizationMethodInterceptor());
  }

  @Override
  public List<ConstructorInterceptor> getConstructorInterceptors(Constructor<?> constructor) {
    return Collections.emptyList();
  }

  /**
   * Through dynamic proxy, obtain the annotations on the method and parameter list to perform
   * metadata authorization.
   */
  private static class MetadataAuthorizationMethodInterceptor implements MethodInterceptor {

    /**
     * Determine whether authorization is required and the rules via the authorization annotation ,
     * and obtain the metadata ID that requires authorization via the authorization annotation.
     *
     * @param methodInvocation methodInvocation with the Method object
     * @return the return result of the original method.
     * @throws Throwable throw an exception when authorization fails.
     */
    @Override
    public Object invoke(MethodInvocation methodInvocation) throws Throwable {
      throw new UnsupportedOperationException();
    }
  }
}
