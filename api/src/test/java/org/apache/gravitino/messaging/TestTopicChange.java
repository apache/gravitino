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

package org.apache.gravitino.messaging;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestTopicChange {

  @Test
  void testRemovePropertyChange() {
    String property = "property1";
    TopicChange topicChange = TopicChange.removeProperty(property);

    Assertions.assertInstanceOf(TopicChange.RemoveProperty.class, topicChange);
    TopicChange.RemoveProperty removeProperty = (TopicChange.RemoveProperty) topicChange;
    Assertions.assertEquals(property, removeProperty.getProperty());
  }

  @Test
  void testUpdateCommentChange() {
    String comment = "comment1";
    TopicChange topicChange = TopicChange.updateComment(comment);

    Assertions.assertInstanceOf(TopicChange.UpdateTopicComment.class, topicChange);
    TopicChange.UpdateTopicComment updateComment = (TopicChange.UpdateTopicComment) topicChange;
    Assertions.assertEquals(comment, updateComment.getNewComment());
  }

  @Test
  void testSetPropertyChange() {
    String property = "property1";
    String value = "value1";
    TopicChange topicChange = TopicChange.setProperty(property, value);

    Assertions.assertInstanceOf(TopicChange.SetProperty.class, topicChange);
    TopicChange.SetProperty setProperty = (TopicChange.SetProperty) topicChange;
    Assertions.assertEquals(property, setProperty.getProperty());
    Assertions.assertEquals(value, setProperty.getValue());
  }

  @Test
  void testEqualsAndHashCode() {
    // remove property
    String property = "property1";
    TopicChange removeProperty1 = TopicChange.removeProperty(property);
    TopicChange removeProperty2 = TopicChange.removeProperty(property);
    TopicChange removeProperty3 = TopicChange.removeProperty("property2");

    Assertions.assertEquals(removeProperty1, removeProperty2);
    Assertions.assertEquals(removeProperty1.hashCode(), removeProperty2.hashCode());

    Assertions.assertNotEquals(removeProperty1, removeProperty3);
    Assertions.assertNotEquals(removeProperty1.hashCode(), removeProperty3.hashCode());

    Assertions.assertNotEquals(removeProperty2, removeProperty3);
    Assertions.assertNotEquals(removeProperty2.hashCode(), removeProperty3.hashCode());

    // update comment
    String comment = "comment1";
    TopicChange updateComment1 = TopicChange.updateComment(comment);
    TopicChange updateComment2 = TopicChange.updateComment(comment);
    TopicChange updateComment3 = TopicChange.updateComment("comment2");

    Assertions.assertEquals(updateComment1, updateComment2);
    Assertions.assertEquals(updateComment1.hashCode(), updateComment2.hashCode());

    Assertions.assertNotEquals(updateComment1, updateComment3);
    Assertions.assertNotEquals(updateComment1.hashCode(), updateComment3.hashCode());

    Assertions.assertNotEquals(updateComment2, updateComment3);
    Assertions.assertNotEquals(updateComment2.hashCode(), updateComment3.hashCode());

    // set property
    String value = "value1";
    TopicChange setProperty1 = TopicChange.setProperty(property, value);
    TopicChange setProperty2 = TopicChange.setProperty(property, value);
    TopicChange setProperty3 = TopicChange.setProperty("property2", "value2");

    Assertions.assertEquals(setProperty1, setProperty2);
    Assertions.assertEquals(setProperty1.hashCode(), setProperty2.hashCode());

    Assertions.assertNotEquals(setProperty1, setProperty3);
    Assertions.assertNotEquals(setProperty1.hashCode(), setProperty3.hashCode());

    Assertions.assertNotEquals(setProperty2, setProperty3);
    Assertions.assertNotEquals(setProperty2.hashCode(), setProperty3.hashCode());
  }
}
