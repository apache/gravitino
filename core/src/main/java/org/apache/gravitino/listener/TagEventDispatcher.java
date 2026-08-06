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
package org.apache.gravitino.listener;

import java.util.Map;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.exceptions.NoSuchTagException;
import org.apache.gravitino.exceptions.TagAlreadyExistsException;
import org.apache.gravitino.listener.api.event.AlterTagEvent;
import org.apache.gravitino.listener.api.event.AlterTagFailureEvent;
import org.apache.gravitino.listener.api.event.AlterTagPreEvent;
import org.apache.gravitino.listener.api.event.AssociateTagsForMetadataObjectEvent;
import org.apache.gravitino.listener.api.event.AssociateTagsForMetadataObjectFailureEvent;
import org.apache.gravitino.listener.api.event.AssociateTagsForMetadataObjectPreEvent;
import org.apache.gravitino.listener.api.event.CreateTagEvent;
import org.apache.gravitino.listener.api.event.CreateTagFailureEvent;
import org.apache.gravitino.listener.api.event.CreateTagPreEvent;
import org.apache.gravitino.listener.api.event.DeleteTagEvent;
import org.apache.gravitino.listener.api.event.DeleteTagFailureEvent;
import org.apache.gravitino.listener.api.event.DeleteTagPreEvent;
import org.apache.gravitino.listener.api.event.GetTagEvent;
import org.apache.gravitino.listener.api.event.GetTagFailureEvent;
import org.apache.gravitino.listener.api.event.GetTagForMetadataObjectEvent;
import org.apache.gravitino.listener.api.event.GetTagForMetadataObjectFailureEvent;
import org.apache.gravitino.listener.api.event.GetTagForMetadataObjectPreEvent;
import org.apache.gravitino.listener.api.event.GetTagPreEvent;
import org.apache.gravitino.listener.api.event.ListMetadataObjectsForTagEvent;
import org.apache.gravitino.listener.api.event.ListMetadataObjectsForTagFailureEvent;
import org.apache.gravitino.listener.api.event.ListMetadataObjectsForTagPreEvent;
import org.apache.gravitino.listener.api.event.ListTagsEvent;
import org.apache.gravitino.listener.api.event.ListTagsFailureEvent;
import org.apache.gravitino.listener.api.event.ListTagsForMetadataObjectEvent;
import org.apache.gravitino.listener.api.event.ListTagsForMetadataObjectFailureEvent;
import org.apache.gravitino.listener.api.event.ListTagsForMetadataObjectPreEvent;
import org.apache.gravitino.listener.api.event.ListTagsInfoEvent;
import org.apache.gravitino.listener.api.event.ListTagsInfoFailureEvent;
import org.apache.gravitino.listener.api.event.ListTagsInfoForMetadataObjectEvent;
import org.apache.gravitino.listener.api.event.ListTagsInfoForMetadataObjectFailureEvent;
import org.apache.gravitino.listener.api.event.ListTagsInfoForMetadataObjectPreEvent;
import org.apache.gravitino.listener.api.event.ListTagsInfoPreEvent;
import org.apache.gravitino.listener.api.event.ListTagsPreEvent;
import org.apache.gravitino.listener.api.info.TagInfo;
import org.apache.gravitino.tag.Tag;
import org.apache.gravitino.tag.TagChange;
import org.apache.gravitino.tag.TagDispatcher;
import org.apache.gravitino.tag.TagValue;
import org.apache.gravitino.tag.TagValueConstraint;
import org.apache.gravitino.utils.PrincipalUtils;

/**
 * {@code TagEventDispatcher} is a decorator for {@link TagDispatcher} that not only delegates tag
 * operations to the underlying tag dispatcher but also dispatches corresponding events to an {@link
 * EventBus} after each operation is completed. This allows for event-driven workflows or monitoring
 * of tag operations.
 */
public class TagEventDispatcher implements TagDispatcher {
  private final EventBus eventBus;
  private final TagDispatcher dispatcher;

  public TagEventDispatcher(EventBus eventBus, TagDispatcher dispatcher) {
    this.eventBus = eventBus;
    this.dispatcher = dispatcher;
  }

  @Override
  public String[] listTags(String metalake) {
    eventBus.dispatchEvent(new ListTagsPreEvent(PrincipalUtils.getCurrentUserName(), metalake));
    try {
      String[] tagNames = dispatcher.listTags(metalake);
      eventBus.dispatchEvent(
          new ListTagsEvent(
              PrincipalUtils.getCurrentUserName(),
              metalake,
              tagNames != null ? tagNames.length : -1));
      return tagNames;
    } catch (Exception e) {
      eventBus.dispatchEvent(
          new ListTagsFailureEvent(PrincipalUtils.getCurrentUserName(), metalake, e));
      throw e;
    }
  }

  @Override
  public Tag[] listTagsInfo(String metalake) {
    eventBus.dispatchEvent(new ListTagsInfoPreEvent(PrincipalUtils.getCurrentUserName(), metalake));
    try {
      Tag[] tags = dispatcher.listTagsInfo(metalake);
      eventBus.dispatchEvent(
          new ListTagsInfoEvent(
              PrincipalUtils.getCurrentUserName(), metalake, tags != null ? tags.length : -1));
      return tags;
    } catch (Exception e) {
      eventBus.dispatchEvent(
          new ListTagsInfoFailureEvent(PrincipalUtils.getCurrentUserName(), metalake, e));
      throw e;
    }
  }

  @Override
  public Tag getTag(String metalake, String name) throws NoSuchTagException {
    eventBus.dispatchEvent(new GetTagPreEvent(PrincipalUtils.getCurrentUserName(), metalake, name));
    try {
      Tag tag = dispatcher.getTag(metalake, name);
      TagInfo tagInfo = tagInfo(tag);
      eventBus.dispatchEvent(
          new GetTagEvent(PrincipalUtils.getCurrentUserName(), metalake, name, tagInfo));
      return tag;
    } catch (Exception e) {
      eventBus.dispatchEvent(
          new GetTagFailureEvent(PrincipalUtils.getCurrentUserName(), metalake, name, e));
      throw e;
    }
  }

  @Override
  public Tag createTag(
      String metalake, String name, String comment, Map<String, String> properties) {
    return createTag(metalake, name, comment, properties, TagValueConstraint.anyValue());
  }

  @Override
  public Tag createTag(
      String metalake,
      String name,
      String comment,
      Map<String, String> properties,
      TagValueConstraint valueConstraint) {
    TagInfo tagInfo =
        new TagInfo(name, comment, properties, allowedValuesForInfo(valueConstraint), null);
    eventBus.dispatchEvent(
        new CreateTagPreEvent(PrincipalUtils.getCurrentUserName(), metalake, tagInfo));
    try {
      Tag tag = dispatcher.createTag(metalake, name, comment, properties, valueConstraint);
      eventBus.dispatchEvent(
          new CreateTagEvent(PrincipalUtils.getCurrentUserName(), metalake, tagInfo(tag)));
      return tag;
    } catch (Exception e) {
      eventBus.dispatchEvent(
          new CreateTagFailureEvent(PrincipalUtils.getCurrentUserName(), metalake, tagInfo, e));
      throw e;
    }
  }

  @Override
  public Tag alterTag(String metalake, String name, TagChange... changes)
      throws IllegalArgumentException, TagAlreadyExistsException {
    AlterTagPreEvent preEvent =
        new AlterTagPreEvent(PrincipalUtils.getCurrentUserName(), metalake, name, changes);

    eventBus.dispatchEvent(preEvent);
    try {
      Tag tag = dispatcher.alterTag(metalake, name, changes);
      eventBus.dispatchEvent(
          new AlterTagEvent(PrincipalUtils.getCurrentUserName(), metalake, changes, tagInfo(tag)));
      return tag;
    } catch (Exception e) {
      eventBus.dispatchEvent(
          new AlterTagFailureEvent(
              PrincipalUtils.getCurrentUserName(), metalake, name, changes, e));
      throw e;
    }
  }

  @Override
  public boolean deleteTag(String metalake, String name) {
    DeleteTagPreEvent preEvent =
        new DeleteTagPreEvent(PrincipalUtils.getCurrentUserName(), metalake, name);

    eventBus.dispatchEvent(preEvent);
    try {
      boolean isExists = dispatcher.deleteTag(metalake, name);
      eventBus.dispatchEvent(
          new DeleteTagEvent(PrincipalUtils.getCurrentUserName(), metalake, name, isExists));
      return isExists;
    } catch (Exception e) {
      eventBus.dispatchEvent(
          new DeleteTagFailureEvent(PrincipalUtils.getCurrentUserName(), metalake, name, e));
      throw e;
    }
  }

  @Override
  public MetadataObject[] listMetadataObjectsForTag(String metalake, String name) {
    eventBus.dispatchEvent(
        new ListMetadataObjectsForTagPreEvent(PrincipalUtils.getCurrentUserName(), metalake, name));
    try {
      MetadataObject[] metadataObjects = dispatcher.listMetadataObjectsForTag(metalake, name);
      eventBus.dispatchEvent(
          new ListMetadataObjectsForTagEvent(
              PrincipalUtils.getCurrentUserName(),
              metalake,
              name,
              metadataObjects != null ? metadataObjects.length : -1));
      return metadataObjects;
    } catch (Exception e) {
      eventBus.dispatchEvent(
          new ListMetadataObjectsForTagFailureEvent(
              PrincipalUtils.getCurrentUserName(), metalake, name, e));
      throw e;
    }
  }

  @Override
  public MetadataObject[] listMetadataObjectsForTag(String metalake, String name, String value) {
    eventBus.dispatchEvent(
        new ListMetadataObjectsForTagPreEvent(PrincipalUtils.getCurrentUserName(), metalake, name));
    try {
      MetadataObject[] metadataObjects =
          dispatcher.listMetadataObjectsForTag(metalake, name, value);
      eventBus.dispatchEvent(
          new ListMetadataObjectsForTagEvent(
              PrincipalUtils.getCurrentUserName(),
              metalake,
              name,
              metadataObjects != null ? metadataObjects.length : -1));
      return metadataObjects;
    } catch (Exception e) {
      eventBus.dispatchEvent(
          new ListMetadataObjectsForTagFailureEvent(
              PrincipalUtils.getCurrentUserName(), metalake, name, e));
      throw e;
    }
  }

  @Override
  public String[] listTagsForMetadataObject(String metalake, MetadataObject metadataObject) {
    eventBus.dispatchEvent(
        new ListTagsForMetadataObjectPreEvent(
            PrincipalUtils.getCurrentUserName(), metalake, metadataObject));

    try {
      String[] tags = dispatcher.listTagsForMetadataObject(metalake, metadataObject);
      eventBus.dispatchEvent(
          new ListTagsForMetadataObjectEvent(
              PrincipalUtils.getCurrentUserName(),
              metalake,
              metadataObject,
              tags != null ? tags.length : -1));
      return tags;
    } catch (Exception e) {
      eventBus.dispatchEvent(
          new ListTagsForMetadataObjectFailureEvent(
              PrincipalUtils.getCurrentUserName(), metalake, metadataObject, e));
      throw e;
    }
  }

  @Override
  public Tag[] listTagsInfoForMetadataObject(String metalake, MetadataObject metadataObject) {
    eventBus.dispatchEvent(
        new ListTagsInfoForMetadataObjectPreEvent(
            PrincipalUtils.getCurrentUserName(), metalake, metadataObject));
    try {
      Tag[] tags = dispatcher.listTagsInfoForMetadataObject(metalake, metadataObject);
      eventBus.dispatchEvent(
          new ListTagsInfoForMetadataObjectEvent(
              PrincipalUtils.getCurrentUserName(),
              metalake,
              metadataObject,
              tags != null ? tags.length : -1));
      return tags;
    } catch (Exception e) {
      eventBus.dispatchEvent(
          new ListTagsInfoForMetadataObjectFailureEvent(
              PrincipalUtils.getCurrentUserName(), metalake, metadataObject, e));
      throw e;
    }
  }

  @Override
  public String[] associateTagsForMetadataObject(
      String metalake, MetadataObject metadataObject, String[] tagsToAdd, String[] tagsToRemove) {
    eventBus.dispatchEvent(
        new AssociateTagsForMetadataObjectPreEvent(
            PrincipalUtils.getCurrentUserName(),
            metalake,
            metadataObject,
            tagsToAdd,
            tagsToRemove));

    try {
      String[] associatedTags =
          dispatcher.associateTagsForMetadataObject(
              metalake, metadataObject, tagsToAdd, tagsToRemove);
      eventBus.dispatchEvent(
          new AssociateTagsForMetadataObjectEvent(
              PrincipalUtils.getCurrentUserName(),
              metalake,
              metadataObject,
              tagsToAdd,
              tagsToRemove,
              associatedTags));
      return associatedTags;
    } catch (Exception e) {
      eventBus.dispatchEvent(
          new AssociateTagsForMetadataObjectFailureEvent(
              PrincipalUtils.getCurrentUserName(),
              metalake,
              metadataObject,
              tagsToAdd,
              tagsToRemove,
              e));
      throw e;
    }
  }

  @Override
  public String[] associateTagValuesForMetadataObject(
      String metalake,
      MetadataObject metadataObject,
      TagValue[] tagsToAdd,
      TagValue[] tagsToRemove) {
    eventBus.dispatchEvent(
        new AssociateTagsForMetadataObjectPreEvent(
            PrincipalUtils.getCurrentUserName(),
            metalake,
            metadataObject,
            tagsToAdd,
            tagsToRemove));

    try {
      String[] associatedTags =
          dispatcher.associateTagValuesForMetadataObject(
              metalake, metadataObject, tagsToAdd, tagsToRemove);
      eventBus.dispatchEvent(
          new AssociateTagsForMetadataObjectEvent(
              PrincipalUtils.getCurrentUserName(),
              metalake,
              metadataObject,
              tagsToAdd,
              tagsToRemove,
              associatedTags));
      return associatedTags;
    } catch (Exception e) {
      eventBus.dispatchEvent(
          new AssociateTagsForMetadataObjectFailureEvent(
              PrincipalUtils.getCurrentUserName(),
              metalake,
              metadataObject,
              tagsToAdd,
              tagsToRemove,
              e));
      throw e;
    }
  }

  @Override
  public Tag getTagForMetadataObject(String metalake, MetadataObject metadataObject, String name) {
    eventBus.dispatchEvent(
        new GetTagForMetadataObjectPreEvent(
            PrincipalUtils.getCurrentUserName(), metalake, metadataObject, name));
    try {
      Tag tag = dispatcher.getTagForMetadataObject(metalake, metadataObject, name);
      TagInfo tagInfo = tagInfo(tag);
      eventBus.dispatchEvent(
          new GetTagForMetadataObjectEvent(
              PrincipalUtils.getCurrentUserName(), metalake, metadataObject, tagInfo));
      return tag;
    } catch (Exception e) {
      eventBus.dispatchEvent(
          new GetTagForMetadataObjectFailureEvent(
              PrincipalUtils.getCurrentUserName(), metalake, metadataObject, name, e));
      throw e;
    }
  }

  private static TagInfo tagInfo(Tag tag) {
    return new TagInfo(
        tag.name(),
        tag.comment(),
        tag.properties(),
        allowedValuesForInfo(tag.valueConstraint()),
        tag.assignment().map(assignment -> assignment.values()).orElse(null));
  }

  private static String[] allowedValuesForInfo(TagValueConstraint valueConstraint) {
    TagValueConstraint normalizedConstraint =
        valueConstraint == null ? TagValueConstraint.anyValue() : valueConstraint;
    switch (normalizedConstraint.type()) {
      case ANY_VALUE:
        return null;
      case NO_VALUE:
      case ALLOWED_VALUES:
        return normalizedConstraint.allowedValues();
      default:
        throw new IllegalArgumentException("Unknown tag value constraint: " + normalizedConstraint);
    }
  }
}
