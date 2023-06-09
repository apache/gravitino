package com.datastrato.graviton.meta;

import com.datastrato.graviton.EntityCreate;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.experimental.Accessors;
import java.util.Map;

public interface LakehouseCreate extends EntityCreate {

  static LakehouseCreate create(String name, String comment, Map<String, String> properties,
                                String creator) {
    return new CreateLakehouse(name, comment, properties, creator);
  }

  @Getter
  @Accessors(fluent = true)
  @EqualsAndHashCode
  final class CreateLakehouse implements LakehouseCreate {

    private final String name;

    private final String comment;

    private final Map<String, String> properties;

    private final String creator;

    private CreateLakehouse(String name, String comment, Map<String, String> properties,
                            String creator) {
      this.name = name;
      this.comment = comment;
      this.properties = properties;
      this.creator = creator;
    }
  }
}
