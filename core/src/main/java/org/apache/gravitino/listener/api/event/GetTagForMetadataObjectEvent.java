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
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.tag.Tag;

/** Represents an event that is triggered upon successfully retrieving a tag for a metadata object. */
@DeveloperApi
public final class GetTagForMetadataObjectEvent extends TagEvent {
    private final String metalake;
    private final MetadataObject metadataObject;
    private final String tagName;
    private final Tag tag;

    /**
     * Constructs an instance of {@code GetTagForMetadataObjectEvent}.
     *
     * @param user The username of the individual who initiated the tag retrieval.
     * @param metalake The metalake from which the tag was retrieved.
     * @param metadataObject The metadata object for which the tag was retrieved.
     * @param tagName The name of the tag being retrieved.
     * @param tag The {@link Tag} object representing the retrieved tag.
     */
    public GetTagForMetadataObjectEvent(String user, String metalake, MetadataObject metadataObject, String tagName, Tag tag) {
        super(user, NameIdentifier.of(metalake));
        this.metalake = metalake;
        this.metadataObject = metadataObject;
        this.tagName = tagName;
        this.tag = tag;
    }

    /**
     * Provides the metalake associated with this event.
     *
     * @return The metalake from which the tag was retrieved.
     */
    public String metalake() {
        return metalake;
    }

    /**
     * Provides the metadata object associated with this event.
     *
     * @return The {@link MetadataObject} for which the tag was retrieved.
     */
    public MetadataObject metadataObject() {
        return metadataObject;
    }

    /**
     * Provides the name of the retrieved tag.
     *
     * @return The name of the tag.
     */
    public String tagName() {
        return tagName;
    }

    /**
     * Provides the retrieved tag object.
     *
     * @return The {@link Tag} object.
     */
    public Tag tag() {
        return tag;
    }

    /**
     * Returns the type of operation.
     *
     * @return The operation type.
     */
    @Override
    public OperationType operationType() {
        return OperationType.GET_TAG_FOR_METADATA_OBJECT;
    }
}
