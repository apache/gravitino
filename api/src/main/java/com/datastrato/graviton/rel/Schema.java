/*·Copyright·2023·Datastrato.·This·software·is·licensed·under·the·Apache·License·version·2.·*/
package com.datastrato.graviton.rel;

import com.datastrato.graviton.Auditable;
import java.util.Collections;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * An interface representing a schema in the {@link com.datastrato.graviton.Catalog}. Schema is a
 * basic container of relational objects, like tables, views, etc. Schema can be self-nested, which
 * means it can be schema1.schema2.table.
 *
 * <p>It defines basic properties of a schema. A catalog implementation with {@link SupportsSchemas}
 * should implement it.
 */
public interface Schema extends Auditable {

  /** return the name of the Schema. */
  String name();

  /** return the comment of the Schema. Null is returned if no comment is set. */
  @Nullable
  default String comment() {
    return null;
  }

  /** return the properties of the Schema. Empty map is returned if no properties are set. */
  default Map<String, String> properties() {
    return Collections.emptyMap();
  }
}
