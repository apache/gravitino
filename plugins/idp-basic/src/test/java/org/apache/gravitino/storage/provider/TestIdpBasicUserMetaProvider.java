/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino.storage.provider;

import java.util.List;
import java.util.ServiceLoader;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestIdpBasicUserMetaProvider {

  @Test
  public void testSingletonAndTypeBridge() {
    IdpBasicUserMetaProvider provider = IdpBasicUserMetaProvider.getInstance();

    Assertions.assertSame(provider, IdpBasicUserMetaProvider.getInstance());
    Assertions.assertInstanceOf(IdpUserMetaProvider.class, provider);
    Assertions.assertInstanceOf(
        org.apache.gravitino.storage.relational.provider.IdpUserMetaProvider.class, provider);
  }

  @Test
  public void testServiceLoaderRegistration() {
    List<Class<?>> providerClasses =
        ServiceLoader.load(
                org.apache.gravitino.storage.relational.provider.IdpUserMetaProvider.class)
            .stream()
            .map(ServiceLoader.Provider::type)
            .collect(Collectors.toList());

    Assertions.assertTrue(providerClasses.contains(IdpBasicUserMetaProvider.class));
  }
}
