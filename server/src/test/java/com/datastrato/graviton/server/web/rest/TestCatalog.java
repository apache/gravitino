package com.datastrato.graviton.server.web.rest;

import com.datastrato.graviton.Config;
import com.datastrato.graviton.meta.BaseCatalog;
import java.io.IOException;

public class TestCatalog extends BaseCatalog {
  @Override
  public void initialize(Config config) throws RuntimeException {}

  @Override
  public void close() throws IOException {}

  public static class Builder extends BaseCatalogBuilder<Builder, TestCatalog> {

    @Override
    protected TestCatalog internalBuild() {
      TestCatalog catalog = new TestCatalog();
      catalog.id = id;
      catalog.metalakeId = metalakeId;
      catalog.name = name;
      catalog.namespace = namespace;
      catalog.type = type;
      catalog.comment = comment;
      catalog.properties = properties;
      catalog.auditInfo = auditInfo;

      return catalog;
    }
  }
}
