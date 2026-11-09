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
package org.apache.gravitino.authorization.common;

import org.apache.gravitino.MetadataObject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestPathBasedMetadataObject {
  @Test
  public void PathBasedMetadataObjectEquals() {
    PathBasedMetadataObject pathBasedMetadataObject1 =
        new PathBasedMetadataObject("parent", "name", "path", PathBasedMetadataObject.FILESET_PATH);
    pathBasedMetadataObject1.validateAuthorizationMetadataObject();

    PathBasedMetadataObject pathBasedMetadataObject2 =
        new PathBasedMetadataObject("parent", "name", "path", PathBasedMetadataObject.FILESET_PATH);
    pathBasedMetadataObject2.validateAuthorizationMetadataObject();

    Assertions.assertEquals(pathBasedMetadataObject1, pathBasedMetadataObject2);
  }

  @Test
  public void PathBasedMetadataObjectNotEquals() {
    PathBasedMetadataObject pathBasedMetadataObject1 =
        new PathBasedMetadataObject("parent", "name", "path", PathBasedMetadataObject.FILESET_PATH);
    pathBasedMetadataObject1.validateAuthorizationMetadataObject();

    PathBasedMetadataObject pathBasedMetadataObject2 =
        new PathBasedMetadataObject(
            "parent", "name", "path1", PathBasedMetadataObject.FILESET_PATH);
    pathBasedMetadataObject2.validateAuthorizationMetadataObject();

    Assertions.assertNotEquals(pathBasedMetadataObject1, pathBasedMetadataObject2);
  }

  @Test
  void testToString() {
    PathBasedMetadataObject pathBasedMetadataObject1 =
        new PathBasedMetadataObject("parent", "name", "path", PathBasedMetadataObject.FILESET_PATH);
    Assertions.assertEquals(
        "MetadataObject: [fullName=parent.name],  [path=path], [type=PATH], [recursive=true]",
        pathBasedMetadataObject1.toString());

    PathBasedMetadataObject pathBasedMetadataObject2 =
        new PathBasedMetadataObject("parent", "name", null, PathBasedMetadataObject.FILESET_PATH);
    Assertions.assertEquals(
        "MetadataObject: [fullName=parent.name],  [path=null], [type=PATH], [recursive=true]",
        pathBasedMetadataObject2.toString());

    PathBasedMetadataObject pathBasedMetadataObject3 =
        new PathBasedMetadataObject(null, "name", null, PathBasedMetadataObject.FILESET_PATH);
    Assertions.assertEquals(
        "MetadataObject: [fullName=name],  [path=null], [type=PATH], [recursive=true]",
        pathBasedMetadataObject3.toString());

    PathBasedMetadataObject pathBasedMetadataObject4 =
        new PathBasedMetadataObject(null, "name", "path", PathBasedMetadataObject.FILESET_PATH);
    Assertions.assertEquals(
        "MetadataObject: [fullName=name],  [path=path], [type=PATH], [recursive=true]",
        pathBasedMetadataObject4.toString());

    PathBasedMetadataObject pathBasedMetadataObject5 =
        new PathBasedMetadataObject(
            null, "name", "path", PathBasedMetadataObject.FILESET_PATH, false);
    Assertions.assertEquals(
        "MetadataObject: [fullName=name],  [path=path], [type=PATH], [recursive=false]",
        pathBasedMetadataObject5.toString());
  }

  @Test
  void testRecursiveFlagAffectsEquality() {
    PathBasedMetadataObject recursiveObject =
        new PathBasedMetadataObject(
            "parent", "name", "path", PathBasedMetadataObject.FILESET_PATH, true);
    PathBasedMetadataObject nonRecursiveObject =
        new PathBasedMetadataObject(
            "parent", "name", "path", PathBasedMetadataObject.FILESET_PATH, false);

    Assertions.assertNotEquals(recursiveObject, nonRecursiveObject);
    Assertions.assertNotEquals(recursiveObject.hashCode(), nonRecursiveObject.hashCode());
  }

  @Test
  public void testEqualsWithDifferentRecursive() {
    PathBasedMetadataObject recursiveObject =
        new PathBasedMetadataObject(
            "parent", "name", "path", PathBasedMetadataObject.FILESET_PATH, true);
    PathBasedMetadataObject recursiveObject2 =
        new PathBasedMetadataObject(
            "parent", "name", "path", PathBasedMetadataObject.FILESET_PATH, true);
    Assertions.assertEquals(recursiveObject, recursiveObject2);
  }

  @Test
  public void testPathTypeGetReturnsCachedInstances() {
    Assertions.assertSame(
        PathBasedMetadataObject.METALAKE_PATH,
        PathBasedMetadataObject.PathType.get(MetadataObject.Type.METALAKE));
    Assertions.assertSame(
        PathBasedMetadataObject.CATALOG_PATH,
        PathBasedMetadataObject.PathType.get(MetadataObject.Type.CATALOG));
    Assertions.assertSame(
        PathBasedMetadataObject.SCHEMA_PATH,
        PathBasedMetadataObject.PathType.get(MetadataObject.Type.SCHEMA));
    Assertions.assertSame(
        PathBasedMetadataObject.TABLE_PATH,
        PathBasedMetadataObject.PathType.get(MetadataObject.Type.TABLE));
    Assertions.assertSame(
        PathBasedMetadataObject.FILESET_PATH,
        PathBasedMetadataObject.PathType.get(MetadataObject.Type.FILESET));
  }

  @Test
  public void testPathTypeGetEquality() {
    Assertions.assertEquals(
        PathBasedMetadataObject.SCHEMA_PATH,
        PathBasedMetadataObject.PathType.get(MetadataObject.Type.SCHEMA));
    Assertions.assertEquals(
        PathBasedMetadataObject.TABLE_PATH,
        PathBasedMetadataObject.PathType.get(MetadataObject.Type.TABLE));
    Assertions.assertNotEquals(
        PathBasedMetadataObject.SCHEMA_PATH,
        PathBasedMetadataObject.PathType.get(MetadataObject.Type.TABLE));
  }

  @Test
  public void testPathTypeGetUnsupportedType() {
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> PathBasedMetadataObject.PathType.get(MetadataObject.Type.COLUMN));
  }

  @Test
  public void testMetadataObjectTypeReferenceComparison() {
    // The path type is a non-enum class, but the cached instances returned by `PathType.get()`
    // guarantee that the reference comparison is correct as well.
    PathBasedMetadataObject schemaMetadataObject =
        new PathBasedMetadataObject(
            "parent",
            "schema",
            "path",
            PathBasedMetadataObject.PathType.get(MetadataObject.Type.SCHEMA));
    schemaMetadataObject.validateAuthorizationMetadataObject();
    Assertions.assertTrue(schemaMetadataObject.type() == PathBasedMetadataObject.SCHEMA_PATH);

    PathBasedMetadataObject tableMetadataObject =
        new PathBasedMetadataObject(
            "parent",
            "table",
            "path",
            PathBasedMetadataObject.PathType.get(MetadataObject.Type.TABLE));
    tableMetadataObject.validateAuthorizationMetadataObject();
    Assertions.assertTrue(tableMetadataObject.type() == PathBasedMetadataObject.TABLE_PATH);
  }
}
