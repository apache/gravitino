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

package org.apache.gravitino.listener.api.event;

import org.apache.gravitino.listener.api.info.TagInfo;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.annotation.DeveloperApi;

@DeveloperApi
public class CreateTagPreEvent extends TagPreEvent {
  private final TagInfo tagInfo;

  public CreateTagPreEvent(String user, String metalake, TagInfo tagInfo) {
    super(user, NameIdentifierUtil.ofTag(metalake, tagInfo.name()));
    this.tagInfo = tagInfo;
  }

  public TagInfo tagInfo() {
    return tagInfo;
  }

  @Override
  public OperationType operationType() {
    return OperationType.CREATE_TAG;
  }
}
