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
package org.apache.gravitino.authorization.jdbc;

import com.google.common.base.Preconditions;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.authorization.AuthorizationMetadataObject;

public class JdbcMetadataObject implements AuthorizationMetadataObject {

  private final String parent;
  private final String name;
  private final MetadataObject.Type type;

  public JdbcMetadataObject(String parent, String name, MetadataObject.Type type) {
    this.parent = parent;
    this.name = name;
    this.type = type;
  }

  @Nullable
  @Override
  public String parent() {
    return parent;
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
  public Type type() {
    return () -> type;
  }

  @Override
  public void validateAuthorizationMetadataObject() throws IllegalArgumentException {
    List<String> names = names();
    Preconditions.checkArgument(
        names != null && !names.isEmpty(), "The name of the object is empty.");
    Preconditions.checkArgument(
        names.size() <= 2, "The name of the object is not in the format of 'database.table'.");
    Preconditions.checkArgument(type != null, "The type of the object is null.");
    Preconditions.checkArgument(
        names.size() == 1 || type == MetadataObject.Type.SCHEMA,
        "The type of the object is not SCHEMA.");
    Preconditions.checkArgument(
        names.size() == 2 || type == MetadataObject.Type.TABLE,
        "The type of the object is not TABLE.");
    for (String name : names) {
      Preconditions.checkArgument(name != null, "Cannot create a metadata object with null name");
    }
  }
}
