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
import org.apache.gravitino.dto.messaging.TopicDTO;
import org.apache.gravitino.exceptions.NoSuchTagException;
import org.apache.gravitino.messaging.Topic;
import org.apache.gravitino.tag.SupportsTags;
import org.apache.gravitino.tag.Tag;

/** Represents a generic topic. */
class GenericTopic implements Topic, SupportsTags {

  private final TopicDTO topicDTO;

  private final MetadataObjectTagOperations objectTagOperations;

  GenericTopic(TopicDTO topicDTO, RESTClient restClient, Namespace topicNs) {
    this.topicDTO = topicDTO;
    List<String> topicFullName =
        Lists.newArrayList(topicNs.level(1), topicNs.level(2), topicDTO.name());
    MetadataObject topicObject = MetadataObjects.of(topicFullName, MetadataObject.Type.TOPIC);
    this.objectTagOperations =
        new MetadataObjectTagOperations(topicNs.level(0), topicObject, restClient);
  }

  @Override
  public Audit auditInfo() {
    return null;
  }

  @Override
  public String name() {
    return topicDTO.name();
  }

  @Override
  public String comment() {
    return topicDTO.comment();
  }

  @Override
  public Map<String, String> properties() {
    return topicDTO.properties();
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
  public String[] associateTags(String[] tagsToAdd, String[] tagsToRemove) {
    return objectTagOperations.associateTags(tagsToAdd, tagsToRemove);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof GenericTopic)) {
      return false;
    }

    GenericTopic that = (GenericTopic) obj;
    return topicDTO.equals(that.topicDTO);
  }

  @Override
  public int hashCode() {
    return topicDTO.hashCode();
  }
}
