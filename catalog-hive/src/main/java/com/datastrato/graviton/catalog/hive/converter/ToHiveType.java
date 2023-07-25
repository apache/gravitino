/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.catalog.hive.converter;

import io.substrait.function.ParameterizedTypeVisitor;
import io.substrait.type.Type;

public class ToHiveType
    extends ParameterizedTypeVisitor.ParameterizedTypeThrowsVisitor<String, RuntimeException> {

  public static ToHiveType INSTANCE = new ToHiveType();

  private ToHiveType() {
    super("Only support type literals and parameterized types.");
  }

  @Override
  public String visit(Type.Bool type) throws RuntimeException {
    return "boolean";
  }

  @Override
  public String visit(Type.I8 type) throws RuntimeException {
    return "tinyint";
  }

  @Override
  public String visit(Type.I16 type) throws RuntimeException {
    return "smallint";
  }

  @Override
  public String visit(Type.I32 type) throws RuntimeException {
    return "int";
  }

  @Override
  public String visit(Type.I64 type) throws RuntimeException {
    return "bigint";
  }

  @Override
  public String visit(Type.FP32 type) throws RuntimeException {
    return "float";
  }

  @Override
  public String visit(Type.FP64 type) throws RuntimeException {
    return "double";
  }

  @Override
  public String visit(Type.Str type) throws RuntimeException {
    return "string";
  }

  @Override
  public String visit(Type.VarChar type) throws RuntimeException {
    return "varchar";
  }

  @Override
  public String visit(Type.FixedChar type) throws RuntimeException {
    return "char";
  }

  @Override
  public String visit(Type.Date type) throws RuntimeException {
    return "date";
  }

  @Override
  public String visit(Type.Time type) throws RuntimeException {
    return "datetime";
  }

  @Override
  public String visit(Type.Timestamp type) throws RuntimeException {
    return "timestamp";
  }

  @Override
  public String visit(Type.TimestampTZ type) throws RuntimeException {
    return "timestamp";
  }

  @Override
  public String visit(Type.Decimal type) throws RuntimeException {
    return "decimal";
  }

  @Override
  public String visit(Type.Binary type) throws RuntimeException {
    return "binary";
  }

  @Override
  public String visit(Type.IntervalYear type) throws RuntimeException {
    return "interval_year_month";
  }

  @Override
  public String visit(Type.IntervalDay type) throws RuntimeException {
    return "interval_day_time";
  }

  @Override
  public String visit(Type.ListType type) throws RuntimeException {
    return String.format("list<%s>", type.elementType().accept(INSTANCE));
  }

  @Override
  public String visit(Type.Map type) throws RuntimeException {
    return String.format("map<%s,%s>", type.key().accept(INSTANCE), type.value().accept(INSTANCE));
  }

  @Override
  public String visit(Type.Struct type) throws RuntimeException {
    // TODO: Implement struct type.
    return super.visit(type);
  }
}
