/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */
package org.apache.gravitino.iceberg.service.dispatcher;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.iceberg.MetadataUpdate;
import org.apache.iceberg.UpdateRequirement;
import org.apache.iceberg.rest.requests.UpdateNamespacePropertiesRequest;
import org.apache.iceberg.rest.requests.UpdateTableRequest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestIcebergOwnerPropertyGuard {

  @Test
  public void testCommitCannotChangeTheOwnerProperty() {
    UpdateTableRequest request =
        new UpdateTableRequest(
            Collections.emptyList(),
            Arrays.asList(
                new MetadataUpdate.SetProperties(ImmutableMap.of("owner", "mallory", "k", "v")),
                new MetadataUpdate.RemoveProperties(ImmutableSet.of("owner")),
                new MetadataUpdate.SetCurrentSchema(0)));

    List<MetadataUpdate> updates = IcebergOwnerPropertyGuard.protect(request, "bob").updates();

    Assertions.assertEquals(2, updates.size());
    Assertions.assertEquals(
        ImmutableMap.of("k", "v"), ((MetadataUpdate.SetProperties) updates.get(0)).updated());
    Assertions.assertInstanceOf(MetadataUpdate.SetCurrentSchema.class, updates.get(1));
  }

  @Test
  public void testCreateCommitSetsTheOwnerToTheCommitter() {
    UpdateTableRequest request =
        new UpdateTableRequest(
            Collections.singletonList(new UpdateRequirement.AssertTableDoesNotExist()),
            Collections.singletonList(
                new MetadataUpdate.SetProperties(ImmutableMap.of("owner", "spark_user"))));

    MetadataUpdate.SetProperties update =
        (MetadataUpdate.SetProperties)
            IcebergOwnerPropertyGuard.protect(request, "bob").updates().get(0);

    Assertions.assertEquals(ImmutableMap.of("owner", "bob"), update.updated());
  }

  @Test
  public void testRequestsWithoutTheOwnerPropertyOrUserAreUnchanged() {
    UpdateTableRequest request =
        new UpdateTableRequest(
            Collections.emptyList(),
            Collections.singletonList(new MetadataUpdate.SetProperties(ImmutableMap.of("k", "v"))));
    Assertions.assertSame(request, IcebergOwnerPropertyGuard.protect(request, "bob"));

    UpdateTableRequest withOwner =
        new UpdateTableRequest(
            Collections.emptyList(),
            Collections.singletonList(
                new MetadataUpdate.SetProperties(ImmutableMap.of("owner", "x"))));
    Assertions.assertSame(withOwner, IcebergOwnerPropertyGuard.protect(withOwner, "anonymous"));
  }

  @Test
  public void testNamespaceUpdateCannotChangeTheOwnerProperty() {
    UpdateNamespacePropertiesRequest request =
        UpdateNamespacePropertiesRequest.builder()
            .update("owner", "mallory")
            .update("k", "v")
            .remove("old")
            .build();

    UpdateNamespacePropertiesRequest protectedRequest =
        IcebergOwnerPropertyGuard.protect(request, "bob");

    Assertions.assertEquals(ImmutableMap.of("k", "v"), protectedRequest.updates());
    Assertions.assertEquals(Collections.singletonList("old"), protectedRequest.removals());

    UpdateNamespacePropertiesRequest removal =
        IcebergOwnerPropertyGuard.protect(
            UpdateNamespacePropertiesRequest.builder().remove("owner").remove("old").build(),
            "bob");
    Assertions.assertEquals(Collections.singletonList("old"), removal.removals());
  }
}
