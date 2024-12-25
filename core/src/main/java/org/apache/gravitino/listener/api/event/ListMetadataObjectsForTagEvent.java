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

/** Represents an event that is triggered upon successfully listing metadata objects for a tag. */
@DeveloperApi
public final class ListMetadataObjectsForTagEvent extends TagEvent {
    private final String metalake;
    private final String tagName;
    private final MetadataObject[] metadataObjects;

    /**
     * Constructs an instance of {@code ListMetadataObjectsForTagEvent}.
     *
     * @param user The username of the individual who initiated the metadata objects listing.
     * @param metalake The metalake from which metadata objects were listed.
     * @param tagName The name of the tag for which metadata objects were listed.
     * @param metadataObjects An array of {@link MetadataObject} representing the listed metadata objects.
     */
    public ListMetadataObjectsForTagEvent(String user, String metalake, String tagName, MetadataObject[] metadataObjects) {
        super(user, NameIdentifier.of(metalake));
        this.metalake = metalake;
        this.tagName = tagName;
        this.metadataObjects = metadataObjects;
    }

    /**
     * Provides the metalake associated with this event.
     *
     * @return The metalake from which metadata objects were listed.
     */
    public String metalake() {
        return metalake;
    }

    /**
     * Provides the name of the tag associated with this event.
     *
     * @return The name of the tag.
     */
    public String tagName() {
        return tagName;
    }

    /**
     * Provides the metadata objects associated with this event.
     *
     * @return An array of {@link MetadataObject}.
     */
    public MetadataObject[] metadataObjects() {
        return metadataObjects;
    }

    /**
     * Returns the type of operation.
     *
     * @return The operation type.
     */
    @Override
    public OperationType operationType() {
        return OperationType.LIST_METADATA_OBJECTS_FOR_TAG;
    }
}
