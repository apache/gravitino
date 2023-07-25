/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.catalog.hive.converter;

import io.substrait.type.Type;
import io.substrait.type.TypeCreator;

public class FromHiveType {
  public static Type convert(String hiveType) throws IllegalArgumentException {
    switch (hiveType) {
      case "boolean":
        return TypeCreator.NULLABLE.BOOLEAN;
      case "tinyint":
        return TypeCreator.NULLABLE.I8;
      case "smallint":
        return TypeCreator.NULLABLE.I16;
      case "int":
        return TypeCreator.NULLABLE.I32;
      case "bigint":
        return TypeCreator.NULLABLE.I64;
      case "float":
        return TypeCreator.NULLABLE.FP32;
      case "double":
        return TypeCreator.NULLABLE.FP64;
      case "string":
        return TypeCreator.NULLABLE.STRING;
      case "varchar":
        return TypeCreator.NULLABLE.varChar(0);
      case "char":
        return TypeCreator.NULLABLE.fixedChar(0);
      case "date":
        return TypeCreator.NULLABLE.DATE;
      case "datetime":
        return TypeCreator.NULLABLE.TIME;
      case "timestamp":
        return TypeCreator.NULLABLE.TIMESTAMP;
      default:
        throw new IllegalArgumentException("Unsupported Hive type: " + hiveType);
    }
  }
}
