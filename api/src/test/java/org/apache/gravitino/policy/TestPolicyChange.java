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

package org.apache.gravitino.policy;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.gravitino.MetadataObject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestPolicyChange {

  @Test
  void testRenamePolicyChange() {
    String name = "name1";
    PolicyChange policyChange = PolicyChange.rename(name);

    Assertions.assertInstanceOf(PolicyChange.RenamePolicy.class, policyChange);
    PolicyChange.RenamePolicy renamePolicy = (PolicyChange.RenamePolicy) policyChange;
    Assertions.assertEquals(name, renamePolicy.getNewName());
  }

  @Test
  void testUpdateCommentChange() {
    String comment = "comment1";
    PolicyChange policyChange = PolicyChange.updateComment(comment);

    Assertions.assertInstanceOf(PolicyChange.UpdatePolicyComment.class, policyChange);
    PolicyChange.UpdatePolicyComment updateComment =
        (PolicyChange.UpdatePolicyComment) policyChange;
    Assertions.assertEquals(comment, updateComment.getNewComment());
  }

  @Test
  void testUpdateContentChange() {
    PolicyContent newContent =
        PolicyContents.custom(
            ImmutableMap.of("rule2", "value2"),
            ImmutableSet.of(MetadataObject.Type.TABLE),
            ImmutableMap.of("key1", "value1"));
    PolicyChange policyChange = PolicyChange.updateContent("custom", newContent);

    Assertions.assertInstanceOf(PolicyChange.UpdateContent.class, policyChange);
    PolicyChange.UpdateContent updateContent = (PolicyChange.UpdateContent) policyChange;
    Assertions.assertEquals("custom", updateContent.getPolicyType());
    Assertions.assertEquals(newContent, updateContent.getContent());
  }

  @Test
  void testEqualsAndHashCode() {
    // RenamePolicy
    PolicyChange rename1 = PolicyChange.rename("name1");
    PolicyChange rename2 = PolicyChange.rename("name1");
    PolicyChange rename3 = PolicyChange.rename("name2");

    Assertions.assertEquals(rename1, rename2);
    Assertions.assertEquals(rename1.hashCode(), rename2.hashCode());

    Assertions.assertNotEquals(rename1, rename3);
    Assertions.assertNotEquals(rename1.hashCode(), rename3.hashCode());

    Assertions.assertNotEquals(rename2, rename3);
    Assertions.assertNotEquals(rename2.hashCode(), rename3.hashCode());

    // UpdateCommentPolicy
    PolicyChange updateComment1 = PolicyChange.updateComment("comment1");
    PolicyChange updateComment2 = PolicyChange.updateComment("comment1");
    PolicyChange updateComment3 = PolicyChange.updateComment("comment2");

    Assertions.assertEquals(updateComment1, updateComment2);
    Assertions.assertEquals(updateComment1.hashCode(), updateComment2.hashCode());

    Assertions.assertNotEquals(updateComment1, updateComment3);
    Assertions.assertNotEquals(updateComment1.hashCode(), updateComment3.hashCode());

    Assertions.assertNotEquals(updateComment2, updateComment3);
    Assertions.assertNotEquals(updateComment2.hashCode(), updateComment3.hashCode());

    // UpdateContentPolicy
    PolicyContent content =
        PolicyContents.custom(
            ImmutableMap.of("rule2", "value2"),
            ImmutableSet.of(MetadataObject.Type.TABLE),
            ImmutableMap.of("key1", "value1"));
    PolicyContent diffContent =
        PolicyContents.custom(
            ImmutableMap.of("rule2", "value2"),
            ImmutableSet.of(MetadataObject.Type.TABLE),
            ImmutableMap.of("key2", "value2"));

    PolicyChange updateContent1 = PolicyChange.updateContent("custom", content);
    PolicyChange updateContent2 = PolicyChange.updateContent("custom", content);
    PolicyChange updateContent3 = PolicyChange.updateContent("custom", diffContent);

    Assertions.assertEquals(updateContent1, updateContent2);
    Assertions.assertEquals(updateContent1.hashCode(), updateContent2.hashCode());

    Assertions.assertNotEquals(updateContent1, updateContent3);

    Assertions.assertNotEquals(updateContent2, updateContent3);
  }
}
