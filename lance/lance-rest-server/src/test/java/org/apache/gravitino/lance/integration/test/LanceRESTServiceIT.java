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
package org.apache.gravitino.lance.integration.test;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.lancedb.lance.namespace.LanceNamespace;
import com.lancedb.lance.namespace.LanceNamespaces;
import com.lancedb.lance.namespace.model.ListNamespacesRequest;
import com.lancedb.lance.namespace.model.ListNamespacesResponse;
import com.lancedb.lance.namespace.rest.RestNamespaceConfig;
import java.util.HashMap;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.integration.test.util.BaseIT;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class LanceRESTServiceIT extends BaseIT {

  private GravitinoMetalake metalake;

  @BeforeAll
  public void startIntegrationTest() throws Exception {
    super.ignoreAuxRestService = false;
    super.startIntegrationTest();
    this.metalake = createMetalake(getLanceRESTServerMetalakeName());
  }

  private GravitinoMetalake createMetalake(String metalakeName) {
    return client.createMetalake(metalakeName, "metalake for lance rest service tests", null);
  }

  private Catalog createCatalog(String catalogName) {
    return metalake.createCatalog(
        catalogName,
        Catalog.Type.RELATIONAL,
        "generic-lakehouse",
        "catalog for lance rest service tests",
        null);
  }

  @Test
  public void testListNamespaces() {
    Catalog catalog1 = createCatalog("lance_catalog_1");
    Catalog catalog2 = createCatalog("lance_catalog_2");

    BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
    HashMap<String, String> props = Maps.newHashMap();
    props.put(RestNamespaceConfig.URI, getLanceRestServiceUrl());

    LanceNamespace ns = LanceNamespaces.connect("rest", props, null, allocator);
    ListNamespacesRequest listNamespacesReq = new ListNamespacesRequest();
    ListNamespacesResponse listNamespacesResp = ns.listNamespaces(listNamespacesReq);

    Assertions.assertEquals(
        Sets.newHashSet(catalog1.name(), catalog2.name()), listNamespacesResp.getNamespaces());
  }

  private String getLanceRestServiceUrl() {
    return String.format("http://%s:%d/lance", "localhost", getLanceRESTServerPort());
  }
}
