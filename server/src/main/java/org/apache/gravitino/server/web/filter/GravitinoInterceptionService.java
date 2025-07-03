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
import com.google.common.collect.ImmutableSet;
import java.lang.reflect.Method;
import java.util.List;
import org.aopalliance.intercept.MethodInterceptor;
import org.apache.gravitino.server.web.rest.CatalogOperations;
import org.apache.gravitino.server.web.rest.FilesetOperations;
import org.apache.gravitino.server.web.rest.MetalakeOperations;
import org.apache.gravitino.server.web.rest.ModelOperations;
import org.apache.gravitino.server.web.rest.SchemaOperations;
import org.apache.gravitino.server.web.rest.TableOperations;
import org.apache.gravitino.server.web.rest.TopicOperations;
import org.glassfish.hk2.api.Filter;

/**
 * GravitinoInterceptionService defines a method interceptor for REST interfaces to create dynamic
 * proxies. It implements metadata authorization when invoking REST API methods. It needs to be
 * registered in the hk2 bean container when the gravitino server starts.
 */
public class GravitinoInterceptionService extends BaseInterceptionService {

  @Override
  public Filter getDescriptorFilter() {
    return new ClassListFilter(
        ImmutableSet.of(
            MetalakeOperations.class.getName(),
            CatalogOperations.class.getName(),
            SchemaOperations.class.getName(),
            TableOperations.class.getName(),
            ModelOperations.class.getName(),
            TopicOperations.class.getName(),
            FilesetOperations.class.getName()));
  }

  @Override
  public List<MethodInterceptor> getMethodInterceptors(Method method) {
    return ImmutableList.of(new GravitinoMetadataAuthorizationMethodInterceptor());
  }
}
