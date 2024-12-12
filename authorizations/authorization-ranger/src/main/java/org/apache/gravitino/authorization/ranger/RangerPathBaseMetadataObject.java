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
package org.apache.gravitino.authorization.ranger;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.authorization.AuthorizationMetadataObject;

public class RangerPathBaseMetadataObject implements AuthorizationMetadataObject {
  /**
   * The type of object in the Ranger system. Every type will map one kind of the entity of the
   * Gravitino type system.
   */
  public enum Type implements AuthorizationMetadataObject.Type {
    /** A path is mapped the path of storages like HDFS, S3 etc. */
    PATH(MetadataObject.Type.FILESET);
    private final MetadataObject.Type metadataType;

    Type(MetadataObject.Type type) {
      this.metadataType = type;
    }

    public MetadataObject.Type metadataObjectType() {
      return metadataType;
    }

    public static RangerHadoopSQLMetadataObject.Type fromMetadataType(
        MetadataObject.Type metadataType) {
      for (RangerHadoopSQLMetadataObject.Type type : RangerHadoopSQLMetadataObject.Type.values()) {
        if (type.metadataObjectType() == metadataType) {
          return type;
        }
      }
      throw new IllegalArgumentException(
          "No matching RangerMetadataObject.Type for " + metadataType);
    }
  }

  private final String path;

  private final AuthorizationMetadataObject.Type type;

  public RangerPathBaseMetadataObject(String path, AuthorizationMetadataObject.Type type) {
    this.path = path;
    this.type = type;
  }

  @Nullable
  @Override
  public String parent() {
    return null;
  }

  @Override
  public String name() {
    return this.path;
  }

  @Override
  public List<String> names() {
    return ImmutableList.of(this.path);
  }

  @Override
  public AuthorizationMetadataObject.Type type() {
    return this.type;
  }

  @Override
  public void validateAuthorizationMetadataObject() throws IllegalArgumentException {
    List<String> names = names();
    Preconditions.checkArgument(
        names != null && !names.isEmpty(), "Cannot create a Ranger metadata object with no names");
    Preconditions.checkArgument(
        names.size() == 1,
        "Cannot create a Ranger metadata object with the name length which is 1");
    Preconditions.checkArgument(
        type != null, "Cannot create a Ranger metadata object with no type");

    Preconditions.checkArgument(
        type == RangerPathBaseMetadataObject.Type.PATH, "it must be the PATH type");

    for (String name : names) {
      Preconditions.checkArgument(name != null, "Cannot create a metadata object with null name");
    }
  }
}
