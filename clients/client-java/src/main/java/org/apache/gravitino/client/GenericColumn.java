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
package org.apache.gravitino.client;

import com.google.common.collect.Lists;
import java.util.Objects;
import org.apache.gravitino.Audit;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.exceptions.NoSuchTagException;
import org.apache.gravitino.exceptions.TagAlreadyAssociatedException;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.expressions.Expression;
import org.apache.gravitino.rel.types.Type;
import org.apache.gravitino.tag.SupportsTags;
import org.apache.gravitino.tag.Tag;

/** Represents a generic column. */
public class GenericColumn implements Column, SupportsTags {

  private final Column internalColumn;

  private final MetadataObjectTagOperations objectTagOperations;

  GenericColumn(
      Column column,
      RESTClient restClient,
      String metalake,
      String catalog,
      String schema,
      String table) {
    this.internalColumn = column;
    MetadataObject columnObject =
        MetadataObjects.of(
            Lists.newArrayList(catalog, schema, table, internalColumn.name()),
            MetadataObject.Type.COLUMN);
    this.objectTagOperations = new MetadataObjectTagOperations(metalake, columnObject, restClient);
  }

  @Override
  public SupportsTags supportsTags() {
    return this;
  }

  @Override
  public String[] listTags() {
    return objectTagOperations.listTags();
  }

  @Override
  public Tag[] listTagsInfo() {
    return objectTagOperations.listTagsInfo();
  }

  @Override
  public Tag getTag(String name) throws NoSuchTagException {
    return objectTagOperations.getTag(name);
  }

  @Override
  public String[] associateTags(String[] tagsToAdd, String[] tagsToRemove)
      throws TagAlreadyAssociatedException {
    return objectTagOperations.associateTags(tagsToAdd, tagsToRemove);
  }

  @Override
  public String name() {
    return internalColumn.name();
  }

  @Override
  public Type dataType() {
    return internalColumn.dataType();
  }

  @Override
  public String comment() {
    return internalColumn.comment();
  }

  @Override
  public boolean nullable() {
    return internalColumn.nullable();
  }

  @Override
  public boolean autoIncrement() {
    return internalColumn.autoIncrement();
  }

  @Override
  public Expression defaultValue() {
    return internalColumn.defaultValue();
  }

  @Override
  public Audit auditInfo() {
    return internalColumn.auditInfo();
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof GenericColumn)) {
      return false;
    }

    GenericColumn column = (GenericColumn) obj;
    return Objects.equals(internalColumn, column.internalColumn);
  }

  @Override
  public int hashCode() {
    return internalColumn.hashCode();
  }
}
