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

import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.annotation.DeveloperApi;
import org.apache.gravitino.listener.api.info.TagInfo;

/**
 * Represents an event that is activated upon the successful creation of a tag.
 */
@DeveloperApi
public final class CreateTagEvent extends TagEvent {
    private final TagInfo createdTagInfo;
    private final String metalake;

    /**
     * Constructs an instance of {@code CreateTagEvent}, capturing essential details about the
     * successful creation of a tag.
     *
     * @param user The username of the individual who initiated the tag creation.
     * @param metalake The metalake from which the tag was created.
     * @param createdTagInfo The final state of the tag post-creation.
     */
    public CreateTagEvent(String user, String metalake, TagInfo createdTagInfo) {
        super(user, NameIdentifier.of(metalake));
        this.metalake = metalake;
        this.createdTagInfo = createdTagInfo;
    }

    /**
     * Provides the final state of the tag as it is presented to the user following the successful
     * creation.
     *
     * @return A {@link TagInfo} object that encapsulates the detailed characteristics of the
     *     newly created tag.
     */
    public TagInfo createdTagInfo() {
        return createdTagInfo;
    }

    /**
     * Returns the type of operation.
     *
     * @return The operation type.
     */
    @Override
    public OperationType operationType() {
        return OperationType.CREATE_TAG;
    }
}

