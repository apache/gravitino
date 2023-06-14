package com.datastrato.graviton.meta;

import com.datastrato.graviton.EntityCreate;
import java.util.Map;
import lombok.EqualsAndHashCode;
import lombok.Getter;

@Getter
@EqualsAndHashCode
public final class CatalogCreate implements EntityCreate {

  private final String name;

  private final String lakehouseName;

  private final BaseCatalog.Type type;

  private final String comment;

  private final Map<String, String> properties;

  private final String creator;

  private CatalogCreate(
      String name,
      String lakehouseName,
      BaseCatalog.Type type,
      String comment,
      Map<String, String> properties,
      String creator) {
    this.name = name;
    this.lakehouseName = lakehouseName;
    this.type = type;
    this.comment = comment;
    this.properties = properties;
    this.creator = creator;
  }

  public static CatalogCreate create(
      String name,
      String lakehouseName,
      BaseCatalog.Type type,
      String comment,
      Map<String, String> properties,
      String creator) {
    return new CatalogCreate(name, lakehouseName, type, comment, properties, creator);
  }
}
