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

import java.util.Arrays;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.RelationalEntity;
import org.apache.gravitino.exceptions.NoSuchTagException;
import org.apache.gravitino.exceptions.TagAlreadyExistsException;
import org.apache.gravitino.policy.PolicyAssociationSelector;

/**
 * {@code TagDispatcher} interface provides functionalities for managing tags within a metalake. It
 * includes a comprehensive set of operations such as listing, creating, retrieving, updating, and
 * deleting tags, as well as associating tags with other objects.
 */
public interface TagDispatcher {
  /**
   * List all the tag names for the specific object.
   *
   * @param metalake object identifier
   * @return The list of tag names.
   */
  String[] listTags(String metalake);

  /**
   * List all the tags with details for the specific object.
   *
   * @param metalake object identifier
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
   * Create a new tag in the specified metalake.
   *
   * @param metalake The name of the metalake
   * @param name The name of the tag
   * @param comment A comment for the new tag.
   * @param properties The properties of the tag.
   * @return The created tag.
   */
  Tag createTag(String metalake, String name, String comment, Map<String, String> properties);

  /**
   * Create a new tag in the specified metalake with an assignment value constraint.
   *
   * @param metalake The name of the metalake.
   * @param name The name of the tag.
   * @param comment A comment for the new tag.
   * @param properties The properties of the tag.
   * @param valueConstraint The assignment value constraint of the tag.
   * @return The created tag.
   */
  default Tag createTag(
      String metalake,
      String name,
      String comment,
      Map<String, String> properties,
      TagValueConstraint valueConstraint) {
    return createTag(metalake, name, comment, properties);
  }

  /**
   * Alter an existing tag in the specified metalake
   *
   * @param metalake The name of the metalake.
   * @param name The name of the tag.
   * @param changes The changes to apply to the tag.
   * @return The updated tag.
   * @throws IllegalArgumentException If the changes cannot be applied to the tag.
   * @throws TagAlreadyExistsException If a tag with the new name already exists.
   */
  Tag alterTag(String metalake, String name, TagChange... changes);

  /**
   * delete an existing tag in the specified metalake
   *
   * @param metalake The name of the metalake.
   * @param name The name of the tag.
   * @return True if the tag was successfully deleted, false otherwise
   */
  boolean deleteTag(String metalake, String name);

  /**
   * List all metadata objects associated with the specified tag.
   *
   * @param metalake The name of the metalake.
   * @param name The name of the tag.
   * @return The array of metadata objects associated with the specified tag.
   */
  MetadataObject[] listMetadataObjectsForTag(String metalake, String name);

  /**
   * List policy names directly associated with the specified tag.
   *
   * @param metalake The name of the metalake.
   * @param name The name of the tag.
   * @return The directly associated policy names.
   */
  default String[] listPoliciesForTag(String metalake, String name) {
    return Arrays.stream(listPolicyAssociationsForTag(metalake, name))
        .map(association -> association.targetEntity().name())
        .toArray(String[]::new);
  }

  /**
   * List policy associations, including selectors, for the specified tag.
   *
   * @param metalake The name of the metalake.
   * @param name The name of the tag.
   * @return The policy-to-tag associations.
   */
  default RelationalEntity<?>[] listPolicyAssociationsForTag(String metalake, String name) {
    throw new UnsupportedOperationException("Listing policy associations is not supported");
  }

  /**
   * Add one policy association for a tag.
   *
   * @param metalake The name of the metalake.
   * @param tagName The name of the tag.
   * @param policyName The name of the policy.
   * @param selector The non-null policy association selector.
   */
  default void addPolicyForTag(
      String metalake, String tagName, String policyName, PolicyAssociationSelector selector) {
    throw new UnsupportedOperationException("Adding a policy for a tag is not supported");
  }

  /**
   * Remove one policy association from a tag.
   *
   * <p>Removing a missing association is an idempotent no-op. The policy and tag must still exist.
   *
   * @param metalake The name of the metalake.
   * @param tagName The name of the tag.
   * @param policyName The name of the policy.
   */
  default void removePolicyFromTag(String metalake, String tagName, String policyName) {
    throw new UnsupportedOperationException("Removing a policy from a tag is not supported");
  }

  /**
   * List all metadata objects associated with the specified tag and exact assignment value.
   *
   * @param metalake The name of the metalake.
   * @param name The name of the tag.
   * @param value The exact assignment value to match, or null to return all objects for the tag.
   * @return The array of metadata objects associated with the specified tag and value.
   */
  default MetadataObject[] listMetadataObjectsForTag(
      String metalake, String name, @Nullable String value) {
    return listMetadataObjectsForTag(metalake, name);
  }

  /**
   * List all tag names associated with the specified metadata object.
   *
   * @param metalake The name of the metalake
   * @param metadataObject The metadata object for which associated tags
   * @return The list of tag names associated with the given metadata object.
   */
  String[] listTagsForMetadataObject(String metalake, MetadataObject metadataObject);

  /**
   * List detailed information for all tags associated with the specified metadata object.
   *
   * @param metalake The name of the metalake
   * @param metadataObject The metadata object to query tag details for.
   * @return An array of tags with detailed information.
   */
  Tag[] listTagsInfoForMetadataObject(String metalake, MetadataObject metadataObject);

  /**
   * Associate or disassociate tags with the specified metadata object.
   *
   * @param metalake The name of the metalake.
   * @param metadataObject The metadata object to update tags for.
   * @param tagsToAdd Tags to associate with the object.
   * @param tagsToRemove Tags to disassociate from the object.
   * @return An array of updated tag names.
   */
  String[] associateTagsForMetadataObject(
      String metalake, MetadataObject metadataObject, String[] tagsToAdd, String[] tagsToRemove);

  /**
   * Associate or disassociate tag values with the specified metadata object.
   *
   * @param metalake The name of the metalake.
   * @param metadataObject The metadata object to update tags for.
   * @param tagsToAdd Tag values to associate with the object.
   * @param tagsToRemove Tag values to disassociate from the object.
   * @return An array of updated tag names.
   */
  default String[] associateTagValuesForMetadataObject(
      String metalake,
      MetadataObject metadataObject,
      TagValue[] tagsToAdd,
      TagValue[] tagsToRemove) {
    String[] tagNamesToAdd =
        tagsToAdd == null
            ? null
            : Arrays.stream(tagsToAdd).map(TagValue::name).toArray(String[]::new);
    String[] tagNamesToRemove =
        tagsToRemove == null
            ? null
            : Arrays.stream(tagsToRemove).map(TagValue::name).toArray(String[]::new);
    return associateTagsForMetadataObject(
        metalake, metadataObject, tagNamesToAdd, tagNamesToRemove);
  }

  /**
   * Retrieve a specific tag associated with the specified metadata object.
   *
   * @param metalake The name of the metalake.
   * @param metadataObject The metadata object to query the tag for.
   * @param name The name of the tag to retrieve.
   * @return The tag associated with the metadata object.
   */
  Tag getTagForMetadataObject(String metalake, MetadataObject metadataObject, String name);
}
