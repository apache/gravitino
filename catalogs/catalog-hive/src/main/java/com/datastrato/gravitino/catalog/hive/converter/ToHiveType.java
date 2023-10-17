/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.hive.converter;

import static org.apache.hadoop.hive.serde.serdeConstants.BIGINT_TYPE_NAME;
import static org.apache.hadoop.hive.serde.serdeConstants.BINARY_TYPE_NAME;
import static org.apache.hadoop.hive.serde.serdeConstants.BOOLEAN_TYPE_NAME;
import static org.apache.hadoop.hive.serde.serdeConstants.DATE_TYPE_NAME;
import static org.apache.hadoop.hive.serde.serdeConstants.DOUBLE_TYPE_NAME;
import static org.apache.hadoop.hive.serde.serdeConstants.FLOAT_TYPE_NAME;
import static org.apache.hadoop.hive.serde.serdeConstants.INTERVAL_DAY_TIME_TYPE_NAME;
import static org.apache.hadoop.hive.serde.serdeConstants.INTERVAL_YEAR_MONTH_TYPE_NAME;
import static org.apache.hadoop.hive.serde.serdeConstants.INT_TYPE_NAME;
import static org.apache.hadoop.hive.serde.serdeConstants.SMALLINT_TYPE_NAME;
import static org.apache.hadoop.hive.serde.serdeConstants.STRING_TYPE_NAME;
import static org.apache.hadoop.hive.serde.serdeConstants.TIMESTAMP_TYPE_NAME;
import static org.apache.hadoop.hive.serde.serdeConstants.TINYINT_TYPE_NAME;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.getCharTypeInfo;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.getDecimalTypeInfo;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.getListTypeInfo;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.getMapTypeInfo;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.getPrimitiveTypeInfo;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.getStructTypeInfo;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.getVarcharTypeInfo;

import io.substrait.function.ParameterizedTypeVisitor;
import io.substrait.type.Type;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

/** Converts Substrait data types to corresponding Hive data types. */
public class ToHiveType
    extends ParameterizedTypeVisitor.ParameterizedTypeThrowsVisitor<TypeInfo, RuntimeException> {

  public static ToHiveType INSTANCE = new ToHiveType();

  private ToHiveType() {
    super("Only support type literals and parameterized types.");
  }

  // Visit methods for each Substrait data type
  @Override
  public TypeInfo visit(Type.Bool type) throws RuntimeException {
    return getPrimitiveTypeInfo(BOOLEAN_TYPE_NAME);
  }

  @Override
  public TypeInfo visit(Type.I8 type) throws RuntimeException {
    return getPrimitiveTypeInfo(TINYINT_TYPE_NAME);
  }

  @Override
  public TypeInfo visit(Type.I16 type) throws RuntimeException {
    return getPrimitiveTypeInfo(SMALLINT_TYPE_NAME);
  }

  @Override
  public TypeInfo visit(Type.I32 type) throws RuntimeException {
    return getPrimitiveTypeInfo(INT_TYPE_NAME);
  }

  @Override
  public TypeInfo visit(Type.I64 type) throws RuntimeException {
    return getPrimitiveTypeInfo(BIGINT_TYPE_NAME);
  }

  @Override
  public TypeInfo visit(Type.FP32 type) throws RuntimeException {
    return getPrimitiveTypeInfo(FLOAT_TYPE_NAME);
  }

  @Override
  public TypeInfo visit(Type.FP64 type) throws RuntimeException {
    return getPrimitiveTypeInfo(DOUBLE_TYPE_NAME);
  }

  @Override
  public TypeInfo visit(Type.Str type) throws RuntimeException {
    return getPrimitiveTypeInfo(STRING_TYPE_NAME);
  }

  @Override
  public TypeInfo visit(Type.VarChar type) throws RuntimeException {
    return getVarcharTypeInfo(type.length());
  }

  @Override
  public TypeInfo visit(Type.FixedChar type) throws RuntimeException {
    return getCharTypeInfo(type.length());
  }

  @Override
  public TypeInfo visit(Type.Date type) throws RuntimeException {
    return getPrimitiveTypeInfo(DATE_TYPE_NAME);
  }

  @Override
  public TypeInfo visit(Type.Timestamp type) throws RuntimeException {
    return getPrimitiveTypeInfo(TIMESTAMP_TYPE_NAME);
  }

  @Override
  public TypeInfo visit(Type.Decimal type) throws RuntimeException {
    return getDecimalTypeInfo(type.precision(), type.scale());
  }

  @Override
  public TypeInfo visit(Type.Binary type) throws RuntimeException {
    return getPrimitiveTypeInfo(BINARY_TYPE_NAME);
  }

  @Override
  public TypeInfo visit(Type.IntervalYear type) throws RuntimeException {
    return getPrimitiveTypeInfo(INTERVAL_YEAR_MONTH_TYPE_NAME);
  }

  @Override
  public TypeInfo visit(Type.IntervalDay type) throws RuntimeException {
    return getPrimitiveTypeInfo(INTERVAL_DAY_TIME_TYPE_NAME);
  }

  @Override
  public TypeInfo visit(Type.ListType type) throws RuntimeException {
    return getListTypeInfo(type.elementType().accept(INSTANCE));
  }

  @Override
  public TypeInfo visit(Type.Map type) throws RuntimeException {
    return getMapTypeInfo(type.key().accept(INSTANCE), type.value().accept(INSTANCE));
  }

  @Override
  public TypeInfo visit(Type.Struct type) throws RuntimeException {
    List<TypeInfo> typeInfos =
        type.fields().stream().map(t -> t.accept(INSTANCE)).collect(Collectors.toList());
    List<String> names =
        IntStream.range(0, typeInfos.size()).mapToObj(String::valueOf).collect(Collectors.toList());
    // TODO: Actually, Hive's Struct type should correspond to Substrait's NamedStruct type.
    //  However, NamedStruct is a Pseudo-type, not an implementation of io.substrait.type.Type.
    //  I haven't figured out a good way to resolve this yet. For now, I'm using an index instead of
    //  name as a temporary workaround.
    return getStructTypeInfo(names, typeInfos);
  }
}
