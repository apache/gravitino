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

import org.apache.gravitino.annotation.Evolving;
import org.apache.gravitino.exceptions.NoSuchTagException;
import org.apache.gravitino.exceptions.TagAlreadyAssociatedException;

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
  String[] listTags();

  /**
   * List all the tags with details for the specific object.
   *
   * @return The list of tags.
   */
  Tag[] listTagsInfo();

  /**
   * Get a tag by its name for the specific object.
   *
   * @param name The name of the tag.
   * @return The tag.
   * @throws NoSuchTagException If the tag does not associate with the object.
   */
  Tag getTag(String name) throws NoSuchTagException;

  /**
   * Associate tags to the specific object. The tagsToAdd will be added to the object, and the
   * tagsToRemove will be removed from the object. Note that: 1) Adding or removing tags that are
   * not existed will be ignored. 2) If the same name tag is in both tagsToAdd and tagsToRemove, it
   * will be ignored. 3) If the tag is already associated with the object, it will throw {@link
   * TagAlreadyAssociatedException}
   *
   * @param tagsToAdd The arrays of tag name to be added to the object.
   * @param tagsToRemove The array of tag name to be removed from the object.
   * @return The array of tag names that are associated with the object.
   * @throws TagAlreadyAssociatedException If the tag is already associated with the object.
   */
  String[] associateTags(String[] tagsToAdd, String[] tagsToRemove)
      throws TagAlreadyAssociatedException;
}
