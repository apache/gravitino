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
package org.apache.gravitino.catalog.fileset;

import static org.apache.gravitino.file.Fileset.PROPERTY_ALLOW_EXISTING_LOCATION_AS_MANAGED;

import com.google.common.collect.ImmutableMap;
import org.apache.gravitino.catalog.PropertiesMetadataHelpers;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Tests the fileset catalog's property validation contract. */
public class TestFilesetPropertiesMetadata {

  /** Checks that the alter validator rejects both setting and removing the adoption property. */
  @Test
  public void testManagedLocationAdoptionPropertyCannotBeAltered() {
    FilesetPropertiesMetadata metadata = new FilesetPropertiesMetadata();
    IllegalArgumentException setFailure =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                PropertiesMetadataHelpers.validatePropertyForAlter(
                    metadata,
                    ImmutableMap.of(PROPERTY_ALLOW_EXISTING_LOCATION_AS_MANAGED, "true"),
                    ImmutableMap.of()));
    Assertions.assertEquals(
        "Property "
            + PROPERTY_ALLOW_EXISTING_LOCATION_AS_MANAGED
            + " is immutable or reserved, cannot be set",
        setFailure.getMessage());

    IllegalArgumentException removeFailure =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                PropertiesMetadataHelpers.validatePropertyForAlter(
                    metadata,
                    ImmutableMap.of(),
                    ImmutableMap.of(
                        PROPERTY_ALLOW_EXISTING_LOCATION_AS_MANAGED,
                        PROPERTY_ALLOW_EXISTING_LOCATION_AS_MANAGED)));
    Assertions.assertEquals(
        "Property "
            + PROPERTY_ALLOW_EXISTING_LOCATION_AS_MANAGED
            + " is immutable or reserved, cannot be deleted",
        removeFailure.getMessage());
  }
}
