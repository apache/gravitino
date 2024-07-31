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

import java.util.Map;
import org.apache.gravitino.annotation.Evolving;
import org.apache.gravitino.exceptions.NoSuchMetalakeException;
import org.apache.gravitino.exceptions.NoSuchTagException;
import org.apache.gravitino.exceptions.TagAlreadyExistsException;

/**
 * Interface for supporting global tag operations. This interface will provide tag listing, getting,
 * creating, and other tag operations under a metalake. This interface will be mixed with
 * GravitinoMetalake or GravitinoClient to provide tag operations.
 */
@Evolving
public interface TagOperations {

  /**
   * List all the tag names under a metalake.
   *
   * @return The list of tag names.
   * @throws NoSuchMetalakeException If the metalake does not exist.
   */
  String[] listTags() throws NoSuchMetalakeException;

  /**
   * List all the tags with detailed information under a metalake.
   *
   * @return The list of tags.
   * @throws NoSuchMetalakeException If the metalake does not exist.
   */
  Tag[] listTagsInfo() throws NoSuchMetalakeException;

  /**
   * Get a tag by its name under a metalake.
   *
   * @param name The name of the tag.
   * @return The tag.
   * @throws NoSuchTagException If the tag does not exist.
   */
  Tag getTag(String name) throws NoSuchTagException;

  /**
   * Create a tag under a metalake.
   *
   * @param name The name of the tag.
   * @param comment The comment of the tag.
   * @param properties The properties of the tag.
   * @return The created tag.
   * @throws TagAlreadyExistsException If the tag already exists.
   */
  Tag createTag(String name, String comment, Map<String, String> properties)
      throws TagAlreadyExistsException;

  /**
   * Alter a tag under a metalake.
   *
   * @param name The name of the tag.
   * @param changes The changes to apply to the tag.
   * @return The altered tag.
   * @throws NoSuchTagException If the tag does not exist.
   * @throws IllegalArgumentException If the changes cannot be applied to the tag.
   */
  Tag alterTag(String name, TagChange... changes)
      throws NoSuchTagException, IllegalArgumentException;

  /**
   * Delete a tag under a metalake.
   *
   * @param name The name of the tag.
   * @return True if the tag is deleted, false if the tag does not exist.
   */
  boolean deleteTag(String name);
}
