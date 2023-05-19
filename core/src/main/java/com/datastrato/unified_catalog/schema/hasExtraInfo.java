package com.datastrato.unified_catalog.schema;

/** Interface for entities that have extra info. */
public interface hasExtraInfo {
  interface ExtraInfo extends Entity {}

  /**
   * Return the extra info of the entity.
   *
   * @return the extra info of the entity.
   */
  ExtraInfo extraInfo();
}
