/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.tag;

import com.datastrato.gravitino.annotation.Evolving;
import com.datastrato.gravitino.exceptions.NoSuchTagException;

/**
 * Interface for supporting getting or associate tags to entities. This interface will be mixed with
 * metadata objects to provide tag operations.
 */
@Evolving
public interface SupportsTags {

  /**
   * List all the tag names for the specific entity.
   *
   * @return The list of tag names.
   */
  String[] listTags();

  /**
   * List all the tags with details for the specific entity.
   *
   * @return The list of tags.
   */
  Tag[] listTagsInfo();

  /**
   * Get a tag by its name for the specific entity.
   *
   * @param name The name of the tag.
   * @return The tag.
   */
  Tag getTag(String name) throws NoSuchTagException;

  /**
   * Associate tags to the specific entity. The tagsToAdd will be added to the entity, and the
   * tagsToRemove will be removed from the entity.
   *
   * @param tagsToAdd The arrays of tag name to be added to the entity.
   * @param tagsToRemove The array of tag name to be removed from the entity.
   * @return The array of tag names that are associated with the entity.
   */
  String[] associateTags(String[] tagsToAdd, String[] tagsToRemove);
}
