package org.apache.gravitino.authorization.ranger;

import java.util.List;

/** Different underlying datasource have different Ranger metadata object rules */
interface RangerMetadataObjectRule {
  /** Validate different underlying datasource Ranger metadata object */
  void validateRangerMetadataObject(List<String> names, RangerMetadataObject.Type type)
      throws IllegalArgumentException;
}
