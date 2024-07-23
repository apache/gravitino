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

import com.google.common.base.Joiner;
import java.util.Map;
import org.apache.gravitino.Audit;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.dto.messaging.TopicDTO;
import org.apache.gravitino.messaging.Topic;
import org.apache.gravitino.tag.SupportsTags;

/** Represents a generic topic. */
public class GenericTopic implements Topic, SupportsTagOperations {

  private static final Joiner DOT_JOINER = Joiner.on(".");

  private final TopicDTO topicDTO;

  private final RESTClient restClient;

  private final Namespace topicNs;

  GenericTopic(TopicDTO topicDTO, RESTClient restClient, Namespace topicNs) {
    this.topicDTO = topicDTO;
    this.restClient = restClient;
    this.topicNs = topicNs;
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
  public String metalakeName() {
    return topicNs.level(0);
  }

  @Override
  public MetadataObject metadataObject() {
    return MetadataObjects.parse(topicFullName(topicNs, name()), MetadataObject.Type.TOPIC);
  }

  @Override
  public RESTClient restClient() {
    return restClient;
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

  private static String topicFullName(Namespace topicNs, String topicName) {
    return DOT_JOINER.join(topicNs.level(1), topicNs.level(2), topicName);
  }
}
