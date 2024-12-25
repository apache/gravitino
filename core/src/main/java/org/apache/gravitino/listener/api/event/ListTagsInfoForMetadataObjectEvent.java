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

/** Represents an event that is triggered upon successfully listing detailed tag information for a metadata object. */
@DeveloperApi
public final class ListTagsInfoForMetadataObjectEvent extends TagEvent {
    private final String metalake;
    private final MetadataObject metadataObject;
    private final Tag[] tags;

    /**
     * Constructs an instance of {@code ListTagsInfoForMetadataObjectEvent}.
     *
     * @param user The username of the individual who initiated the tag information listing.
     * @param metalake The metalake from which tag information was listed.
     * @param metadataObject The metadata object for which tag information was listed.
     * @param tags An array of {@link Tag} objects representing the detailed tag information.
     */
    public ListTagsInfoForMetadataObjectEvent(String user, String metalake, MetadataObject metadataObject, Tag[] tags) {
        super(user, NameIdentifier.of(metalake));
        this.metalake = metalake;
        this.metadataObject = metadataObject;
        this.tags = tags;
    }

    /**
     * Provides the metalake associated with this event.
     *
     * @return The metalake from which tag information was listed.
     */
    public String metalake() {
        return metalake;
    }

    /**
     * Provides the metadata object associated with this event.
     *
     * @return The {@link MetadataObject} for which tag information was listed.
     */
    public MetadataObject metadataObject() {
        return metadataObject;
    }

    /**
     * Provides the detailed tag information associated with this event.
     *
     * @return An array of {@link Tag} objects.
     */
    public Tag[] tags() {
        return tags;
    }

    /**
     * Returns the type of operation.
     *
     * @return The operation type.
     */
    @Override
    public OperationType operationType() {
        return OperationType.LIST_TAGS_INFO_FOR_METADATA_OBJECT;
    }
}
