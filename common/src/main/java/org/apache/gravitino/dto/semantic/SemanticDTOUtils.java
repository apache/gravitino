/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino.dto.semantic;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.gravitino.semantic.DataType;

final class SemanticDTOUtils {

  private SemanticDTOUtils() {}

  @Nullable
  static <T> T[] copyArray(@Nullable T[] values) {
    return values == null ? null : Arrays.copyOf(values, values.length);
  }

  @Nullable
  static <T> T[][] copy2DArray(@Nullable T[][] values) {
    if (values == null) {
      return null;
    }

    T[][] copied = Arrays.copyOf(values, values.length);
    for (int index = 0; index < values.length; index++) {
      copied[index] = copyArray(values[index]);
    }
    return copied;
  }

  @Nullable
  static <S, T> T[] convertArray(
      @Nullable S[] values, Function<S, T> converter, IntFunction<T[]> arrayFactory) {
    if (values == null) {
      return null;
    }

    T[] converted = arrayFactory.apply(values.length);
    for (int index = 0; index < values.length; index++) {
      converted[index] = values[index] == null ? null : converter.apply(values[index]);
    }
    return converted;
  }

  @Nullable
  static Object readJsonValue(JsonParser parser, JsonToken token) throws IOException {
    switch (token) {
      case VALUE_NULL:
        return null;
      case VALUE_STRING:
        return parser.getText();
      case VALUE_TRUE:
        return Boolean.TRUE;
      case VALUE_FALSE:
        return Boolean.FALSE;
      case VALUE_NUMBER_INT:
        return parser.getBigIntegerValue();
      case VALUE_NUMBER_FLOAT:
        return parser.getDecimalValue();
      case START_OBJECT:
        return readJsonObject(parser);
      case START_ARRAY:
        return readJsonArray(parser);
      default:
        throw JsonMappingException.from(parser, "Unsupported JSON token: " + token);
    }
  }

  static final class DataTypeSerializer extends JsonSerializer<DataType> {

    @Override
    public void serialize(DataType value, JsonGenerator generator, SerializerProvider serializers)
        throws IOException {
      generator.writeString(dataTypeName(value));
    }
  }

  static final class DataTypeDeserializer extends JsonDeserializer<DataType> {

    @Override
    public DataType deserialize(JsonParser parser, DeserializationContext context)
        throws IOException {
      String value = requireString(parser, "DataType");
      switch (value) {
        case "String":
          return DataType.STRING;
        case "Integer":
          return DataType.INTEGER;
        case "Decimal":
          return DataType.DECIMAL;
        case "Float":
          return DataType.FLOAT;
        case "Boolean":
          return DataType.BOOLEAN;
        case "Date":
          return DataType.DATE;
        case "Time":
          return DataType.TIME;
        case "DateTime":
          return DataType.DATE_TIME;
        case "DateTimeTz":
          return DataType.DATE_TIME_TZ;
        case "Opaque":
          return DataType.OPAQUE;
        default:
          throw JsonMappingException.from(
              parser,
              "Unknown Semantic Model data type: "
                  + value
                  + ". Supported values: "
                  + supportedDataTypeNames());
      }
    }
  }

  private static Map<String, Object> readJsonObject(JsonParser parser) throws IOException {
    Map<String, Object> values = new LinkedHashMap<>();
    while (parser.nextToken() != JsonToken.END_OBJECT) {
      if (!parser.hasToken(JsonToken.FIELD_NAME)) {
        throw JsonMappingException.from(
            parser, "Expected a JSON object property name, but found " + parser.currentToken());
      }
      String name = parser.currentName();
      values.put(name, readJsonValue(parser, parser.nextToken()));
    }
    return values;
  }

  private static List<Object> readJsonArray(JsonParser parser) throws IOException {
    List<Object> values = new ArrayList<>();
    while (parser.nextToken() != JsonToken.END_ARRAY) {
      values.add(readJsonValue(parser, parser.currentToken()));
    }
    return values;
  }

  private static String requireString(JsonParser parser, String type) throws IOException {
    if (!parser.hasToken(JsonToken.VALUE_STRING)) {
      throw JsonMappingException.from(
          parser, type + " must be encoded as a string, but found " + parser.currentToken());
    }
    return parser.getText();
  }

  private static String dataTypeName(DataType dataType) {
    switch (dataType) {
      case STRING:
        return "String";
      case INTEGER:
        return "Integer";
      case DECIMAL:
        return "Decimal";
      case FLOAT:
        return "Float";
      case BOOLEAN:
        return "Boolean";
      case DATE:
        return "Date";
      case TIME:
        return "Time";
      case DATE_TIME:
        return "DateTime";
      case DATE_TIME_TZ:
        return "DateTimeTz";
      case OPAQUE:
        return "Opaque";
      default:
        throw new IllegalArgumentException("Unsupported Semantic Model data type: " + dataType);
    }
  }

  private static String supportedDataTypeNames() {
    return Arrays.stream(DataType.values())
        .map(SemanticDTOUtils::dataTypeName)
        .collect(Collectors.joining(", "));
  }
}
