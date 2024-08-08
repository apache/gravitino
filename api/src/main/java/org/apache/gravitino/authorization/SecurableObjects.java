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
package org.apache.gravitino.authorization;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.MetadataObjects.MetadataObjectImpl;

/** The helper class for {@link SecurableObject}. */
public class SecurableObjects {

  private static final Splitter DOT_SPLITTER = Splitter.on('.');

  /**
   * Create the metalake {@link SecurableObject} with the given metalake name and privileges.
   *
   * @param metalake The metalake name
   * @param privileges The privileges of the metalake
   * @return The created metalake {@link SecurableObject}
   */
  public static SecurableObject ofMetalake(String metalake, List<Privilege> privileges) {
    return of(MetadataObject.Type.METALAKE, Lists.newArrayList(metalake), privileges);
  }

  /**
   * Create the catalog {@link SecurableObject} with the given catalog name and privileges.
   *
   * @param catalog The catalog name
   * @param privileges The privileges of the catalog
   * @return The created catalog {@link SecurableObject}
   */
  public static SecurableObject ofCatalog(String catalog, List<Privilege> privileges) {
    return of(MetadataObject.Type.CATALOG, Lists.newArrayList(catalog), privileges);
  }

  /**
   * Create the schema {@link SecurableObject} with the given securable catalog object, schema name
   * and privileges.
   *
   * @param catalog The catalog securable object.
   * @param schema The schema name
   * @param privileges The privileges of the schema
   * @return The created schema {@link SecurableObject}
   */
  public static SecurableObject ofSchema(
      SecurableObject catalog, String schema, List<Privilege> privileges) {
    return of(
        MetadataObject.Type.SCHEMA, Lists.newArrayList(catalog.fullName(), schema), privileges);
  }

  /**
   * Create the table {@link SecurableObject} with the given securable schema object, table name and
   * privileges.
   *
   * @param schema The schema securable object
   * @param table The table name
   * @param privileges The privileges of the table
   * @return The created table {@link SecurableObject}
   */
  public static SecurableObject ofTable(
      SecurableObject schema, String table, List<Privilege> privileges) {
    List<String> names = Lists.newArrayList(DOT_SPLITTER.splitToList(schema.fullName()));
    names.add(table);
    return of(MetadataObject.Type.TABLE, names, privileges);
  }

  /**
   * Create the topic {@link SecurableObject} with the given securable schema object ,topic name and
   * privileges.
   *
   * @param schema The schema securable object
   * @param topic The topic name
   * @param privileges The privileges of the topic
   * @return The created topic {@link SecurableObject}
   */
  public static SecurableObject ofTopic(
      SecurableObject schema, String topic, List<Privilege> privileges) {
    List<String> names = Lists.newArrayList(DOT_SPLITTER.splitToList(schema.fullName()));
    names.add(topic);
    return of(MetadataObject.Type.TOPIC, names, privileges);
  }

  /**
   * Create the table {@link SecurableObject} with the given securable schema object, fileset name
   * and privileges.
   *
   * @param schema The schema securable object
   * @param fileset The fileset name
   * @param privileges The privileges of the fileset
   * @return The created fileset {@link SecurableObject}
   */
  public static SecurableObject ofFileset(
      SecurableObject schema, String fileset, List<Privilege> privileges) {
    List<String> names = Lists.newArrayList(DOT_SPLITTER.splitToList(schema.fullName()));
    names.add(fileset);
    return of(MetadataObject.Type.FILESET, names, privileges);
  }

  private static class SecurableObjectImpl extends MetadataObjectImpl implements SecurableObject {

    private List<Privilege> privileges;

    SecurableObjectImpl(String parent, String name, Type type, List<Privilege> privileges) {
      super(parent, name, type);
      this.privileges = ImmutableList.copyOf(privileges);
    }

    @Override
    public List<Privilege> privileges() {
      return privileges;
    }

    @Override
    public int hashCode() {
      int result = super.hashCode();
      return Objects.hash(result, privileges);
    }

    @Override
    public String toString() {
      String privilegesStr =
          privileges.stream()
              .map(p -> "[" + p.simpleString() + "]")
              .collect(Collectors.joining(","));

      return "SecurableObject: [fullName="
          + fullName()
          + "], [type="
          + type()
          + "], [privileges="
          + privilegesStr
          + "]";
    }

    @Override
    public boolean equals(Object other) {
      if (this == other) {
        return true;
      }

      if (!(other instanceof SecurableObject)) {
        return false;
      }

      SecurableObject otherSecurableObject = (SecurableObject) other;
      return super.equals(other) && Objects.equals(privileges, otherSecurableObject.privileges());
    }
  }

  /**
   * Create a {@link SecurableObject} from the given full name.
   *
   * @param fullName The full name of securable object.
   * @param type The securable object type.
   * @param privileges The securable object privileges.
   * @return The created {@link SecurableObject}
   */
  public static SecurableObject parse(
      String fullName, MetadataObject.Type type, List<Privilege> privileges) {
    MetadataObject metadataObject = MetadataObjects.parse(fullName, type);
    return new SecurableObjectImpl(
        metadataObject.parent(), metadataObject.name(), type, privileges);
  }

  /**
   * Create the {@link SecurableObject} with the given names.
   *
   * @param type The securable object type.
   * @param names The names of the securable object.
   * @param privileges The securable object privileges.
   * @return The created {@link SecurableObject}
   */
  static SecurableObject of(
      MetadataObject.Type type, List<String> names, List<Privilege> privileges) {
    MetadataObject metadataObject = MetadataObjects.of(names, type);
    return new SecurableObjectImpl(
        metadataObject.parent(), metadataObject.name(), type, privileges);
  }
}
