/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.tag;

import com.datastrato.gravitino.annotation.Evolving;
import com.datastrato.gravitino.exceptions.NoSuchTagException;
import com.datastrato.gravitino.exceptions.TagAlreadyExistsException;
import java.util.Map;

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
   */
  String[] listTags();

  /**
   * List all the tags with detailed information under a metalake.
   *
   * @param extended If true, the extended information of the tag will be included.
   * @return The list of tags.
   */
  Tag[] listTagsInfo(boolean extended);

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
