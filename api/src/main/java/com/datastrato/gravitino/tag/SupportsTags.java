/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.tag;

import com.datastrato.gravitino.annotation.Evolving;
import com.datastrato.gravitino.exceptions.NoSuchTagException;

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
   */
  Tag getTag(String name) throws NoSuchTagException;

  /**
   * Associate tags to the specific object. The tagsToAdd will be added to the object, and the
   * tagsToRemove will be removed from the object.
   *
   * @param tagsToAdd The arrays of tag name to be added to the object.
   * @param tagsToRemove The array of tag name to be removed from the object.
   * @return The array of tag names that are associated with the object.
   */
  String[] associateTags(String[] tagsToAdd, String[] tagsToRemove);
}
