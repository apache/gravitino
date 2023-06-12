package com.datastrato.graviton.meta;

import com.datastrato.graviton.Config;
import java.io.IOException;

public class TestCatalog extends BaseCatalog {

  @Override
  public void initialize(Config config) throws RuntimeException {}

  @Override
  public void close() throws IOException {}

  public static class Builder extends BaseCatalog.BaseCatalogBuilder<Builder, TestCatalog> {

    TestCatalog testCatalog = new TestCatalog();

    @Override
    protected TestCatalog internalBuild() {
      testCatalog.id = id;
      testCatalog.lakehouseId = lakehouseId;
      testCatalog.name = name;
      testCatalog.type = type;
      testCatalog.comment = comment;
      testCatalog.properties = properties;
      testCatalog.auditInfo = auditInfo;

      return testCatalog;
    }
  }
}
