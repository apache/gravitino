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

/** Represents an event that is generated after a tag is successfully deleted. */
@DeveloperApi
public final class DeleteTagEvent extends TagEvent {
    private final boolean isExists;

    /**
     * Constructs a new {@code DeleteTagEvent} instance, encapsulating information about the outcome
     * of a tag delete operation.
     *
     * @param user The user who initiated the delete tag operation.
     * @param identifier The identifier of the tag that was attempted to be deleted.
     * @param isExists A boolean flag indicating whether the tag existed at the time of the delete
     *     operation.
     */
    public DeleteTagEvent(String user, NameIdentifier identifier, boolean isExists) {
        super(user, identifier);
        this.isExists = isExists;
    }

    /**
     * Retrieves the existence status of the tag at the time of the delete operation.
     *
     * @return A boolean value indicating whether the tag existed. {@code true} if the tag
     *     existed, otherwise {@code false}.
     */
    public boolean isExists() {
        return isExists;
    }

    /**
     * Returns the type of operation.
     *
     * @return the operation type.
     */
    @Override
    public OperationType operationType() {
        return OperationType.DELETE_TAG;
    }
}
