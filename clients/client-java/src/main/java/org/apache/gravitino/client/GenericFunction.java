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
import org.apache.gravitino.Audit;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.exceptions.NoSuchTagException;
import org.apache.gravitino.exceptions.TagAlreadyAssociatedException;
import org.apache.gravitino.function.Function;
import org.apache.gravitino.function.FunctionDefinition;
import org.apache.gravitino.function.FunctionType;
import org.apache.gravitino.tag.SupportsTags;
import org.apache.gravitino.tag.Tag;

/** Represents a generic function. */
class GenericFunction implements Function, SupportsTags {

  private final Function function;

  private final MetadataObjectTagOperations objectTagOperations;

  GenericFunction(Function function, RESTClient restClient, Namespace functionNs) {
    this.function = function;
    List<String> functionFullName =
        Lists.newArrayList(functionNs.level(1), functionNs.level(2), function.name());
    MetadataObject functionObject =
        MetadataObjects.of(functionFullName, MetadataObject.Type.FUNCTION);
    this.objectTagOperations =
        new MetadataObjectTagOperations(functionNs.level(0), functionObject, restClient);
  }

  @Override
  public Audit auditInfo() {
    return function.auditInfo();
  }

  @Override
  public String name() {
    return function.name();
  }

  @Override
  public FunctionType functionType() {
    return function.functionType();
  }

  @Override
  public boolean deterministic() {
    return function.deterministic();
  }

  @Override
  public String comment() {
    return function.comment();
  }

  @Override
  public FunctionDefinition[] definitions() {
    return function.definitions();
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
    if (!(obj instanceof GenericFunction)) {
      return false;
    }

    GenericFunction that = (GenericFunction) obj;
    return function.equals(that.function);
  }

  @Override
  public int hashCode() {
    return function.hashCode();
  }

  @Override
  public String toString() {
    return "GenericFunction{" + "function=" + function.toString() + '}';
  }
}
