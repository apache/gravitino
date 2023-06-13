package com.datastrato.graviton.meta;

import com.datastrato.graviton.EntityCreate;
import java.util.Map;
import lombok.EqualsAndHashCode;
import lombok.Getter;

@Getter
@EqualsAndHashCode
public final class LakehouseCreate implements EntityCreate {

  private final String name;

  private final String comment;

  private final Map<String, String> properties;

  private final String creator;

  private LakehouseCreate(
      String name, String comment, Map<String, String> properties, String creator) {
    this.name = name;
    this.comment = comment;
    this.properties = properties;
    this.creator = creator;
  }

  public static LakehouseCreate create(
      String name, String comment, Map<String, String> properties, String creator) {
    return new LakehouseCreate(name, comment, properties, creator);
  }
}
