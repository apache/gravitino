package com.datastrato.graviton.meta;

import com.datastrato.graviton.EntityCreate;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.experimental.Accessors;

import java.util.Map;

public interface CatalogCreate extends EntityCreate {

  static CatalogCreate create(String name, String lakehouseName, BaseCatalog.Type type,
                              String comment, Map<String, String> properties, String creator) {
    return new CreateCatalog(name, lakehouseName, type, comment, properties, creator);
  }

  @Getter
  @Accessors(fluent = true)
  @EqualsAndHashCode
  final class CreateCatalog implements CatalogCreate {

    private final String name;

    private final String lakehouseName;

    private final BaseCatalog.Type type;

    private final String comment;

    private final Map<String, String> properties;

    private final String creator;

    private CreateCatalog(String name, String lakehouseName, BaseCatalog.Type type, String comment,
                          Map<String, String> properties, String creator) {
      this.name = name;
      this.lakehouseName = lakehouseName;
      this.type = type;
      this.comment = comment;
      this.properties = properties;
      this.creator = creator;
    }
  }
}
