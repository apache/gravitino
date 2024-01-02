/*
 *  Copyright 2023 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.catalog.lakehouse.iceberg.combine.hive;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import org.apache.iceberg.ClientPool;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.io.FileIO;

/** Mainly used to create CombineHiveTableOperations */
public class CombineHiveCatalog extends HiveCatalog {

  @Override
  public TableOperations newTableOps(TableIdentifier tableIdentifier) {
    String dbName = tableIdentifier.namespace().level(0);
    String tableName = tableIdentifier.name();
    return new CombineHiveTableOperations(
        getConf(), getClientPool(this), getFileIO(this), name(), dbName, tableName);
  }

  private ClientPool getClientPool(CombineHiveCatalog hiveCatalog) {
    try {
      Class metaStoreCatalogClass = hiveCatalog.getClass().getSuperclass();
      Method newTableOps = metaStoreCatalogClass.getDeclaredMethod("clientPool");
      newTableOps.setAccessible(true);
      Object o = newTableOps.invoke(hiveCatalog);
      return (ClientPool) o;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private FileIO getFileIO(CombineHiveCatalog hiveCatalog) {
    try {
      Class<?> clazz = hiveCatalog.getClass().getSuperclass();
      Field privateField = clazz.getDeclaredField("fileIO");
      privateField.setAccessible(true);
      Object fieldValue = privateField.get(hiveCatalog);
      return (FileIO) fieldValue;
    } catch (NoSuchFieldException e) {
      throw new RuntimeException(e);
    } catch (IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }
}
