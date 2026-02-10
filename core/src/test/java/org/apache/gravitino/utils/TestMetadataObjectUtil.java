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
package org.apache.gravitino.utils;

import org.apache.gravitino.Entity;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.NameIdentifier;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestMetadataObjectUtil {

  @Test
  public void testToEntityType() {
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> MetadataObjectUtil.toEntityType((MetadataObject) null),
        "metadataObject cannot be null");

    Assertions.assertEquals(
        Entity.EntityType.METALAKE,
        MetadataObjectUtil.toEntityType(
            MetadataObjects.of(null, "metalake", MetadataObject.Type.METALAKE)));

    Assertions.assertEquals(
        Entity.EntityType.CATALOG,
        MetadataObjectUtil.toEntityType(
            MetadataObjects.of(null, "catalog", MetadataObject.Type.CATALOG)));

    Assertions.assertEquals(
        Entity.EntityType.SCHEMA,
        MetadataObjectUtil.toEntityType(
            MetadataObjects.of("catalog", "schema", MetadataObject.Type.SCHEMA)));

    Assertions.assertEquals(
        Entity.EntityType.TABLE,
        MetadataObjectUtil.toEntityType(
            MetadataObjects.of("catalog.schema", "table", MetadataObject.Type.TABLE)));

    Assertions.assertEquals(
        Entity.EntityType.TOPIC,
        MetadataObjectUtil.toEntityType(
            MetadataObjects.of("catalog.schema", "topic", MetadataObject.Type.TOPIC)));

    Assertions.assertEquals(
        Entity.EntityType.FILESET,
        MetadataObjectUtil.toEntityType(
            MetadataObjects.of("catalog.schema", "fileset", MetadataObject.Type.FILESET)));

    Assertions.assertEquals(
        Entity.EntityType.COLUMN,
        MetadataObjectUtil.toEntityType(
            MetadataObjects.of("catalog.schema.table", "column", MetadataObject.Type.COLUMN)));

    Assertions.assertEquals(
        Entity.EntityType.MODEL,
        MetadataObjectUtil.toEntityType(
            MetadataObjects.of("catalog.schema", "model", MetadataObject.Type.MODEL)));
  }

  @Test
  public void testToEntityIdent() {
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> MetadataObjectUtil.toEntityIdent(null, null),
        "metadataName cannot be blank");

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> MetadataObjectUtil.toEntityIdent("metalake", null),
        "metadataObject cannot be null");

    Assertions.assertEquals(
        NameIdentifier.of("metalake"),
        MetadataObjectUtil.toEntityIdent(
            "metalake", MetadataObjects.of(null, "metalake", MetadataObject.Type.METALAKE)));

    Assertions.assertEquals(
        NameIdentifier.of("metalake", "catalog"),
        MetadataObjectUtil.toEntityIdent(
            "metalake", MetadataObjects.of(null, "catalog", MetadataObject.Type.CATALOG)));

    Assertions.assertEquals(
        NameIdentifier.of("metalake", "catalog", "schema"),
        MetadataObjectUtil.toEntityIdent(
            "metalake", MetadataObjects.of("catalog", "schema", MetadataObject.Type.SCHEMA)));

    Assertions.assertEquals(
        NameIdentifier.of("metalake", "catalog", "schema", "table"),
        MetadataObjectUtil.toEntityIdent(
            "metalake", MetadataObjects.of("catalog.schema", "table", MetadataObject.Type.TABLE)));

    Assertions.assertEquals(
        NameIdentifier.of("metalake", "catalog", "schema", "topic"),
        MetadataObjectUtil.toEntityIdent(
            "metalake", MetadataObjects.of("catalog.schema", "topic", MetadataObject.Type.TOPIC)));

    Assertions.assertEquals(
        NameIdentifier.of("metalake", "catalog", "schema", "fileset"),
        MetadataObjectUtil.toEntityIdent(
            "metalake",
            MetadataObjects.of("catalog.schema", "fileset", MetadataObject.Type.FILESET)));

    Assertions.assertEquals(
        NameIdentifier.of("metalake", "catalog", "schema", "model"),
        MetadataObjectUtil.toEntityIdent(
            "metalake", MetadataObjects.of("catalog.schema", "model", MetadataObject.Type.MODEL)));

    Assertions.assertEquals(
        NameIdentifier.of("metalake", "catalog", "schema", "table", "column"),
        MetadataObjectUtil.toEntityIdent(
            "metalake",
            MetadataObjects.of("catalog.schema.table", "column", MetadataObject.Type.COLUMN)));
  }
}
