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

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import java.util.List;
import org.apache.gravitino.MetadataObject;

/** The helper class for {@link RangerMetadataObject}. */
public class RangerMetadataObjects {
  private static final Splitter DOT_SPLITTER = Splitter.on('.');

  private static final Joiner DOT_JOINER = Joiner.on('.');

  private RangerMetadataObjects() {}

  /**
   * Get the parent full name of the given full name.
   *
   * @param names The names of the metadata object
   * @return The parent full name if it exists, otherwise null
   */
  public static String getParentFullName(List<String> names) {
    if (names.size() <= 1) {
      return null;
    }

    return DOT_JOINER.join(names.subList(0, names.size() - 1));
  }

  static String getLastName(List<String> names) {
    Preconditions.checkArgument(names.size() > 0, "Cannot get the last name of an empty list");
    return names.get(names.size() - 1);
  }

  static void checkName(String name) {
    Preconditions.checkArgument(name != null, "Cannot create a metadata object with null name");
  }

  /** The implementation of the {@link MetadataObject}. */
  public static class RangerMetadataObjectImpl implements RangerMetadataObject {
    private final String name;

    private final String parent;

    private final RangerMetadataObject.Type type;

    /**
     * Create the metadata object with the given name, parent and type.
     *
     * @param parent The parent of the metadata object
     * @param name The name of the metadata object
     * @param type The type of the metadata object
     */
    public RangerMetadataObjectImpl(String parent, String name, RangerMetadataObject.Type type) {
      this.parent = parent;
      this.name = name;
      this.type = type;
    }

    @Override
    public String name() {
      return name;
    }

    @Override
    public List<String> names() {
      return Lists.newArrayList(DOT_SPLITTER.splitToList(fullName()));
    }

    @Override
    public String parent() {
      return parent;
    }

    @Override
    public Type type() {
      return type;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }

      if (!(o instanceof RangerMetadataObjectImpl)) {
        return false;
      }

      RangerMetadataObjectImpl that = (RangerMetadataObjectImpl) o;
      return java.util.Objects.equals(name, that.name)
          && java.util.Objects.equals(parent, that.parent)
          && type == that.type;
    }

    @Override
    public int hashCode() {
      return java.util.Objects.hash(name, parent, type);
    }

    @Override
    public String toString() {
      return "MetadataObject: [fullName=" + fullName() + "], [type=" + type + "]";
    }
  }
}
