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
import java.util.List;
import java.util.Map;
import org.apache.gravitino.Audit;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.exceptions.NoSuchTagException;
import org.apache.gravitino.exceptions.TagAlreadyAssociatedException;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Representation;
import org.apache.gravitino.rel.View;
import org.apache.gravitino.tag.SupportsTags;
import org.apache.gravitino.tag.Tag;

/** Represents a generic view. */
class GenericView implements View, SupportsTags {

  private final View view;

  private final MetadataObjectTagOperations objectTagOperations;

  GenericView(View view, RESTClient restClient, Namespace viewNs) {
    this.view = view;
    List<String> viewFullName = Lists.newArrayList(viewNs.level(1), viewNs.level(2), view.name());
    MetadataObject viewObject = MetadataObjects.of(viewFullName, MetadataObject.Type.VIEW);
    this.objectTagOperations =
        new MetadataObjectTagOperations(viewNs.level(0), viewObject, restClient);
  }

  @Override
  public Audit auditInfo() {
    return view.auditInfo();
  }

  @Override
  public String name() {
    return view.name();
  }

  @Override
  public String comment() {
    return view.comment();
  }

  @Override
  public Column[] columns() {
    return view.columns();
  }

  @Override
  public Representation[] representations() {
    return view.representations();
  }

  @Override
  public String defaultCatalog() {
    return view.defaultCatalog();
  }

  @Override
  public String defaultSchema() {
    return view.defaultSchema();
  }

  @Override
  public Map<String, String> properties() {
    return view.properties();
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
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof GenericView)) {
      return false;
    }

    GenericView that = (GenericView) obj;
    return view.equals(that.view);
  }

  @Override
  public int hashCode() {
    return view.hashCode();
  }

  @Override
  public String toString() {
    return "GenericView{" + "view=" + view.toString() + '}';
  }
}
