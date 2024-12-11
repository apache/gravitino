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

package org.apache.gravitino.tag;

import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.annotation.Evolving;
import org.apache.gravitino.exceptions.NoSuchTagException;

import java.util.Map;

/**
 * Interface for supporting getting or associate tags to objects. This interface will be mixed with
 * metadata objects to provide tag operations.
 */
@Evolving
public interface SupportsTags {

    /**
     * List all the tag names for the specific object.
     *
     * @return The list of tag names.
     */
    String[] listTags(String metalake);

    /**
     * List all the tags with details for the specific object.
     *
     * @return The list of tags.
     */
    Tag[] listTagsInfo(String metalake);

    /**
     * Get a tag by its name for the specific object.
     *
     * @param name The name of the tag.
     * @param metalake The name of the metalake
     * @return The tag.
     * @throws NoSuchTagException If the tag does not associate with the object.
     */
    Tag getTag(String metalake, String name) throws NoSuchTagException;

    /**
     *
     * @param metalake T
     * @param name
     * @param comment
     * @param properties
     * @return
     */
    Tag createTag(String metalake, String name, String comment, Map<String, String> properties);

    /**
     *
     * @param metalake
     * @param name
     * @param changes
     * @return
     */
    Tag alterTag(String metalake, String name, TagChange... changes);

    /**
     *
     * @param metalake
     * @param name
     * @return
     */
    boolean deleteTag(String metalake, String name);

    /**
     *
     * @param metalake
     * @param name
     * @return
     */
    MetadataObject[] listMetadataObjectsForTag(String metalake, String name);

    /**
     *
     * @param metalake
     * @param metadataObject
     * @return
     */
    String[] listTagsForMetadataObject(String metalake, MetadataObject metadataObject);





}
