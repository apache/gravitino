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
package org.apache.gravitino.hook;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.authorization.OwnerDispatcher;
import org.apache.gravitino.tag.Tag;
import org.apache.gravitino.tag.TagDispatcher;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestTagHookDispatcher {

  private TagHookDispatcher hookDispatcher;
  private TagDispatcher mockDispatcher;
  private OwnerDispatcher mockOwnerDispatcher;

  @BeforeEach
  public void setUp() throws IllegalAccessException {
    mockDispatcher = mock(TagDispatcher.class);
    mockOwnerDispatcher = mock(OwnerDispatcher.class);
    FieldUtils.writeField(GravitinoEnv.getInstance(), "ownerDispatcher", mockOwnerDispatcher, true);
    hookDispatcher = new TagHookDispatcher(mockDispatcher);
  }

  @AfterEach
  public void tearDown() throws IllegalAccessException {
    FieldUtils.writeField(GravitinoEnv.getInstance(), "ownerDispatcher", null, true);
  }

  @Test
  public void testCreateTagSucceedsEvenIfSetOwnerFails() {
    Tag mockTag = mock(Tag.class);
    when(mockDispatcher.createTag(any(), any(), any(), any())).thenReturn(mockTag);

    doThrow(new RuntimeException("Set owner failed"))
        .when(mockOwnerDispatcher)
        .setOwner(any(), any(), any(), any());

    Tag result =
        hookDispatcher.createTag("test_metalake", "test_tag", "comment", Collections.emptyMap());

    Assertions.assertEquals(mockTag, result);
    verify(mockDispatcher).createTag(any(), any(), any(), any());
  }
}
