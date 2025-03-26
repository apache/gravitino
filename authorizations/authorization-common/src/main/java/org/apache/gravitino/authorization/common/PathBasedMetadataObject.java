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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.authorization.AuthorizationMetadataObject;

public class PathBasedMetadataObject implements AuthorizationMetadataObject {
  /**
   * The type of metadata object in the underlying system. Every type will map one kind of the
   * entity of the Gravitino type system. When we store a Hive table, first, we will store the
   * metadata in the JDBC database like MySQL, and then we will store the data in the HDFS location.
   * For Hive, there is a default location for the cluster level. We can also specify other
   * locations for schema and table levels. So a path may not be fileset
   */
  public static final class PathType implements AuthorizationMetadataObject.Type {

    private static final Set<MetadataObject.Type> allowObjectTypes =
        ImmutableSet.of(
            MetadataObject.Type.METALAKE,
            MetadataObject.Type.CATALOG,
            MetadataObject.Type.SCHEMA,
            MetadataObject.Type.TABLE,
            MetadataObject.Type.FILESET);
    private final MetadataObject.Type metadataType;

    public static PathType get(MetadataObject.Type type) {
      return new PathType(type);
    }

    private PathType(MetadataObject.Type type) {
      Preconditions.checkArgument(
          allowObjectTypes.contains(type), "The type isn't allow to create path type");
      this.metadataType = type;
    }

    public MetadataObject.Type metadataObjectType() {
      return metadataType;
    }

    @Override
    public int hashCode() {
      return metadataType.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (!(obj instanceof PathType)) {
        return false;
      }

      return this.metadataType.equals(((PathType) obj).metadataType);
    }

    @Override
    public String toString() {
      return "PATH";
    }
  }

  private final String name;
  private final String parent;
  private final String path;

  private final AuthorizationMetadataObject.Type type;

  public PathBasedMetadataObject(
      String parent, String name, String path, AuthorizationMetadataObject.Type type) {
    this.parent = parent;
    this.name = name;
    this.path = path;
    this.type = type;
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public List<String> names() {
    return DOT_SPLITTER.splitToList(fullName());
  }

  @Override
  public String parent() {
    return parent;
  }

  public String path() {
    return path;
  }

  @Override
  public AuthorizationMetadataObject.Type type() {
    return this.type;
  }

  @Override
  public void validateAuthorizationMetadataObject() throws IllegalArgumentException {
    List<String> names = names();
    Preconditions.checkArgument(
        names != null && !names.isEmpty(),
        "Cannot create a path based metadata object with no names");
    Preconditions.checkArgument(
        path != null && !path.isEmpty(), "Cannot create a path based metadata object with no path");

    Preconditions.checkArgument(type instanceof PathType, "it must be the PATH type");

    for (String name : names) {
      Preconditions.checkArgument(
          name != null, "Cannot create a path based metadata object with null name");
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (!(o instanceof PathBasedMetadataObject)) {
      return false;
    }

    PathBasedMetadataObject that = (PathBasedMetadataObject) o;
    return Objects.equals(name, that.name)
        && Objects.equals(parent, that.parent)
        && Objects.equals(path, that.path)
        && Objects.equals(type, that.type);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, parent, path, type);
  }

  @Override
  public String toString() {
    String strPath = path == null ? "null" : path;
    return "MetadataObject: [fullName="
        + fullName()
        + "],  [path="
        + strPath
        + "], [type="
        + type
        + "]";
  }
}
