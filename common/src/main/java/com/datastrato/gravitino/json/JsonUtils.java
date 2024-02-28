/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.json;

import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.dto.rel.DistributionDTO;
import com.datastrato.gravitino.dto.rel.SortOrderDTO;
import com.datastrato.gravitino.dto.rel.expressions.FieldReferenceDTO;
import com.datastrato.gravitino.dto.rel.expressions.FuncExpressionDTO;
import com.datastrato.gravitino.dto.rel.expressions.FunctionArg;
import com.datastrato.gravitino.dto.rel.expressions.LiteralDTO;
import com.datastrato.gravitino.dto.rel.expressions.UnparsedExpressionDTO;
import com.datastrato.gravitino.dto.rel.indexes.IndexDTO;
import com.datastrato.gravitino.dto.rel.partitioning.BucketPartitioningDTO;
import com.datastrato.gravitino.dto.rel.partitioning.DayPartitioningDTO;
import com.datastrato.gravitino.dto.rel.partitioning.FunctionPartitioningDTO;
import com.datastrato.gravitino.dto.rel.partitioning.HourPartitioningDTO;
import com.datastrato.gravitino.dto.rel.partitioning.IdentityPartitioningDTO;
import com.datastrato.gravitino.dto.rel.partitioning.ListPartitioningDTO;
import com.datastrato.gravitino.dto.rel.partitioning.MonthPartitioningDTO;
import com.datastrato.gravitino.dto.rel.partitioning.Partitioning;
import com.datastrato.gravitino.dto.rel.partitioning.RangePartitioningDTO;
import com.datastrato.gravitino.dto.rel.partitioning.TruncatePartitioningDTO;
import com.datastrato.gravitino.dto.rel.partitioning.YearPartitioningDTO;
import com.datastrato.gravitino.dto.rel.partitions.IdentityPartitionDTO;
import com.datastrato.gravitino.dto.rel.partitions.ListPartitionDTO;
import com.datastrato.gravitino.dto.rel.partitions.PartitionDTO;
import com.datastrato.gravitino.dto.rel.partitions.RangePartitionDTO;
import com.datastrato.gravitino.rel.Column;
import com.datastrato.gravitino.rel.TableChange;
import com.datastrato.gravitino.rel.expressions.Expression;
import com.datastrato.gravitino.rel.expressions.UnparsedExpression;
import com.datastrato.gravitino.rel.expressions.distributions.Strategy;
import com.datastrato.gravitino.rel.expressions.sorts.NullOrdering;
import com.datastrato.gravitino.rel.expressions.sorts.SortDirection;
import com.datastrato.gravitino.rel.indexes.Index;
import com.datastrato.gravitino.rel.types.Type;
import com.datastrato.gravitino.rel.types.Types;
import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.cfg.EnumFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Utility class for working with JSON data. */
public class JsonUtils {

  private static final String NAMESPACE = "namespace";
  private static final String NAME = "name";
  private static final String POSITION_FIRST = "first";
  private static final String POSITION_AFTER = "after";
  private static final String POSITION_DEFAULT = "default";
  private static final String STRATEGY = "strategy";
  private static final String FIELD_NAME = "fieldName";
  private static final String FIELD_NAMES = "fieldNames";
  private static final String NUM_BUCKETS = "numBuckets";
  private static final String WIDTH = "width";
  private static final String FUNCTION_NAME = "funcName";
  private static final String FUNCTION_ARGS = "funcArgs";
  private static final String EXPRESSION_TYPE = "type";
  private static final String DATA_TYPE = "dataType";
  private static final String LITERAL_VALUE = "value";
  private static final String UNPARSED_EXPRESSION = "unparsedExpression";
  private static final String SORT_TERM = "sortTerm";
  private static final String DIRECTION = "direction";
  private static final String NULL_ORDERING = "nullOrdering";

  private static final String INDEX_TYPE = "indexType";
  private static final String INDEX_NAME = "name";
  private static final String INDEX_FIELD_NAMES = "fieldNames";
  private static final String NUMBER = "number";
  private static final String TYPE = "type";
  private static final String STRUCT = "struct";
  private static final String LIST = "list";
  private static final String MAP = "map";
  private static final String UNION = "union";
  private static final String UNPARSED = "unparsed";
  private static final String UNPARSED_TYPE = "unparsedType";
  private static final String FIELDS = "fields";
  private static final String UNION_TYPES = "types";
  private static final String STRUCT_FIELD_NAME = "name";
  private static final String STRUCT_FIELD_NULLABLE = "nullable";
  private static final String STRUCT_FIELD_COMMENT = "comment";
  private static final String LIST_ELEMENT_NULLABLE = "containsNull";
  private static final String LIST_ELEMENT_TYPE = "elementType";
  private static final String MAP_KEY_TYPE = "keyType";
  private static final String MAP_VALUE_TYPE = "valueType";
  private static final String MAP_VALUE_NULLABLE = "valueContainsNull";
  private static final String PARTITION_TYPE = "type";
  private static final String PARTITION_NAME = "name";
  private static final String PARTITION_PROPERTIES = "properties";
  private static final String IDENTITY_PARTITION_VALUES = "values";
  private static final String LIST_PARTITION_LISTS = "lists";
  private static final String RANGE_PARTITION_UPPER = "upper";
  private static final String RANGE_PARTITION_LOWER = "lower";
  private static final ImmutableMap<String, Type.PrimitiveType> TYPES =
      Maps.uniqueIndex(
          ImmutableList.of(
              Types.BooleanType.get(),
              Types.ByteType.get(),
              Types.ShortType.get(),
              Types.IntegerType.get(),
              Types.LongType.get(),
              Types.FloatType.get(),
              Types.DoubleType.get(),
              Types.DateType.get(),
              Types.TimeType.get(),
              Types.TimestampType.withTimeZone(),
              Types.TimestampType.withoutTimeZone(),
              Types.StringType.get(),
              Types.UUIDType.get(),
              Types.BinaryType.get(),
              Types.IntervalYearType.get(),
              Types.IntervalDayType.get()),
          Type.PrimitiveType::simpleString);
  private static final Pattern FIXED = Pattern.compile("fixed\\(\\s*(\\d+)\\s*\\)");
  private static final Pattern FIXEDCHAR = Pattern.compile("char\\(\\s*(\\d+)\\s*\\)");
  private static final Pattern VARCHAR = Pattern.compile("varchar\\(\\s*(\\d+)\\s*\\)");
  private static final Pattern DECIMAL =
      Pattern.compile("decimal\\(\\s*(\\d+)\\s*,\\s*(\\d+)\\s*\\)");

  /**
   * Abstract iterator class for iterating over elements of a JSON array.
   *
   * @param <T> The type of elements to convert and iterate.
   */
  abstract static class JsonArrayIterator<T> implements Iterator<T> {

    private final Iterator<JsonNode> elements;

    /**
     * Constructor for the JSON array iterator.
     *
     * @param property The property name of the JSON array.
     * @param node The JSON node containing the array.
     */
    JsonArrayIterator(String property, JsonNode node) {
      JsonNode pNode = node.get(property);
      Preconditions.checkArgument(
          pNode != null && !pNode.isNull() && pNode.isArray(),
          "Cannot parse from non-array value: %s: %s",
          property,
          pNode);
      this.elements = pNode.elements();
    }

    /**
     * Judge whether it has more elements in the JSON array.
     *
     * @return true if the iteration has more elements.
     */
    @Override
    public boolean hasNext() {
      return elements.hasNext();
    }

    /**
     * Get a next element from the JSON array.
     *
     * @return the next element in the iteration.
     */
    @Override
    public T next() {
      JsonNode element = elements.next();
      return convert(element);
    }

    /**
     * Convert a JSON node to the desired element type.
     *
     * @param element The JSON node to convert.
     * @return The converted element.
     */
    abstract T convert(JsonNode element);
  }

  /** Iterator for JSON arrays containing strings. */
  static class JsonStringArrayIterator extends JsonArrayIterator<String> {
    private final String property;

    JsonStringArrayIterator(String property, JsonNode node) {
      super(property, node);
      this.property = property;
    }

    @Override
    String convert(JsonNode element) {
      return convertToString(property, element);
    }
  }

  /**
   * ObjectMapperHolder is a static inner class that holds the instance of ObjectMapper. This class
   * utilizes the Initialization-on-demand holder idiom, which is a lazy-loaded singleton. This
   * idiom takes advantage of the fact that inner classes are not loaded until they are referenced.
   * It's a thread-safe and efficient way to implement a singleton as the instance is created when
   * it's needed at the first time.
   */
  private static class ObjectMapperHolder {
    private static final ObjectMapper INSTANCE =
        JsonMapper.builder()
            .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false)
            .configure(EnumFeature.WRITE_ENUMS_TO_LOWERCASE, true)
            .enable(MapperFeature.ACCEPT_CASE_INSENSITIVE_ENUMS)
            .build()
            .registerModule(new JavaTimeModule());
  }

  /**
   * Get the shared ObjectMapper instance for JSON serialization/deserialization.
   *
   * @return The ObjectMapper instance.
   */
  public static ObjectMapper objectMapper() {
    return ObjectMapperHolder.INSTANCE;
  }

  /**
   * AnyFieldMapperHolder is a static inner class that holds the instance of ObjectMapper which can
   * access any field of the object. This class utilizes the Initialization-on-demand holder idiom,
   * which is a lazy-loaded singleton. This idiom takes advantage of the fact that inner classes are
   * not loaded until they are referenced. It's a thread-safe and efficient way to implement a
   * singleton as the instance is created when it's needed at the first time.
   */
  private static class AnyFieldMapperHolder {
    private static final ObjectMapper INSTANCE =
        JsonMapper.builder()
            .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false)
            .configure(EnumFeature.WRITE_ENUMS_TO_LOWERCASE, true)
            .enable(MapperFeature.ACCEPT_CASE_INSENSITIVE_ENUMS)
            .build()
            .setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY)
            .registerModule(new JavaTimeModule());
  }

  /**
   * Get the shared AnyFieldMapper instance for JSON serialization/deserialization.
   *
   * @return The ObjectMapper instance.
   */
  public static ObjectMapper anyFieldMapper() {
    return AnyFieldMapperHolder.INSTANCE;
  }

  /**
   * Get a list of strings from a JSON node property.
   *
   * @param property The property name.
   * @param node The JSON node.
   * @return The list of strings or null if property is missing or null.
   */
  static List<String> getStringListOrNull(String property, JsonNode node) {
    if (!node.has(property) || node.get(property).isNull()) {
      return null;
    }

    return getStringList(property, node);
  }

  /**
   * Get a list of strings from a JSON node property or throw an exception if the property is
   * missing.
   *
   * @param property The property name.
   * @param node The JSON node.
   * @return The list of strings.
   * @throws IllegalArgumentException if the property is missing in the JSON node.
   */
  private static List<String> getStringList(String property, JsonNode node) {
    Preconditions.checkArgument(node.has(property), "Cannot parse missing property: %s", property);
    return ImmutableList.<String>builder()
        .addAll(new JsonStringArrayIterator(property, node))
        .build();
  }

  private static String[] getStringArray(ArrayNode node) {
    String[] array = new String[node.size()];
    for (int i = 0; i < node.size(); i++) {
      array[i] = node.get(i).asText();
    }
    return array;
  }

  /**
   * Get a int value from a JSON node property.
   *
   * @param property The property name.
   * @param node The JSON node.
   * @return The int value.
   */
  public static int getInt(String property, JsonNode node) {
    Preconditions.checkArgument(node.has(property), "Cannot parse missing property: %s", property);
    JsonNode pNode = node.get(property);
    Preconditions.checkArgument(
        pNode != null && !pNode.isNull() && pNode.isInt(),
        "Cannot parse to an int value %s: %s",
        property,
        pNode);
    return pNode.asInt();
  }

  /**
   * Get a long value from a JSON node property.
   *
   * @param property The property name.
   * @param node The JSON node.
   * @return The long value.
   */
  public static long getLong(String property, JsonNode node) {
    Preconditions.checkArgument(node.has(property), "Cannot parse missing property: %s", property);
    JsonNode pNode = node.get(property);
    Preconditions.checkArgument(
        pNode != null && !pNode.isNull() && pNode.isIntegralNumber() && pNode.canConvertToLong(),
        "Cannot parse to an long value %s: %s",
        property,
        pNode);
    return pNode.asLong();
  }

  private static FunctionArg readFunctionArg(JsonNode node) {
    Preconditions.checkArgument(
        node != null && !node.isNull() && node.isObject(),
        "Cannot parse function arg from invalid JSON: %s",
        node);
    Preconditions.checkArgument(
        node.has(EXPRESSION_TYPE), "Cannot parse function arg from missing type: %s", node);
    String type = getString(EXPRESSION_TYPE, node);
    switch (FunctionArg.ArgType.valueOf(type.toUpperCase())) {
      case LITERAL:
        Preconditions.checkArgument(
            node.has(DATA_TYPE), "Cannot parse literal arg from missing data type: %s", node);
        Preconditions.checkArgument(
            node.has(LITERAL_VALUE),
            "Cannot parse literal arg from missing literal value: %s",
            node);
        Type dataType = readDataType(node.get(DATA_TYPE));
        String value = getStringOrNull(LITERAL_VALUE, node);
        return new LiteralDTO.Builder().withDataType(dataType).withValue(value).build();
      case FIELD:
        Preconditions.checkArgument(
            node.has(FIELD_NAME),
            "Cannot parse field reference arg from missing field name: %s",
            node);
        String[] fieldName = getStringList(FIELD_NAME, node).toArray(new String[0]);
        return FieldReferenceDTO.of(fieldName);
      case FUNCTION:
        Preconditions.checkArgument(
            node.has(FUNCTION_NAME),
            "Cannot parse function function arg from missing function name: %s",
            node);
        Preconditions.checkArgument(
            node.has(FUNCTION_ARGS),
            "Cannot parse function function arg from missing function args: %s",
            node);
        String functionName = getString(FUNCTION_NAME, node);
        List<FunctionArg> args = Lists.newArrayList();
        node.get(FUNCTION_ARGS).forEach(arg -> args.add(readFunctionArg(arg)));
        return new FuncExpressionDTO.Builder()
            .withFunctionName(functionName)
            .withFunctionArgs(args.toArray(FunctionArg.EMPTY_ARGS))
            .build();
      case UNPARSED:
        Preconditions.checkArgument(
            node.has(UNPARSED_EXPRESSION) && node.get(UNPARSED_EXPRESSION).isTextual(),
            "Cannot parse unparsed expression from missing string field unparsedExpression: %s",
            node);
        return UnparsedExpressionDTO.builder()
            .withUnparsedExpression(getString(UNPARSED_EXPRESSION, node))
            .build();
      default:
        throw new IllegalArgumentException("Unknown function argument type: " + type);
    }
  }

  private static void writeFunctionArg(FunctionArg arg, JsonGenerator gen) throws IOException {
    gen.writeStartObject();
    gen.writeStringField(EXPRESSION_TYPE, arg.argType().name().toLowerCase());
    switch (arg.argType()) {
      case LITERAL:
        gen.writeFieldName(DATA_TYPE);
        writeDataType(((LiteralDTO) arg).dataType(), gen);
        gen.writeStringField(LITERAL_VALUE, ((LiteralDTO) arg).value());
        break;
      case FIELD:
        gen.writeFieldName(FIELD_NAME);
        gen.writeObject(((FieldReferenceDTO) arg).fieldName());
        break;
      case FUNCTION:
        gen.writeStringField(FUNCTION_NAME, ((FuncExpressionDTO) arg).functionName());
        gen.writeArrayFieldStart(FUNCTION_ARGS);
        for (FunctionArg funcArg : ((FuncExpressionDTO) arg).args()) {
          writeFunctionArg(funcArg, gen);
        }
        gen.writeEndArray();
        break;
      case UNPARSED:
        gen.writeStringField(UNPARSED_EXPRESSION, ((UnparsedExpression) arg).unparsedExpression());
        break;
      default:
        throw new IOException("Unknown function argument type: " + arg.argType());
    }
    gen.writeEndObject();
  }

  /**
   * Get a string value from a JSON node property.
   *
   * @param property The property name.
   * @param node The JSON node.
   * @return The string value.
   * @throws IllegalArgumentException if the property is missing in the JSON node.
   */
  public static String getString(String property, JsonNode node) {
    Preconditions.checkArgument(node.has(property), "Cannot parse missing string: %s", property);
    JsonNode pNode = node.get(property);
    return convertToString(property, pNode);
  }

  private static String getStringOrNull(String property, JsonNode node) {
    if (!node.has(property) || node.get(property).isNull()) {
      return null;
    }

    return getString(property, node);
  }

  private static Map<String, String> getStringMapOrNull(String property, JsonNode node) {
    if (!node.has(property) || node.get(property).isNull()) {
      return null;
    }

    JsonNode propertiesNode = node.get(property);
    Iterator<Map.Entry<String, JsonNode>> fieldsIterator = propertiesNode.fields();
    Map<String, String> properties = Maps.newHashMap();
    while (fieldsIterator.hasNext()) {
      Map.Entry<String, JsonNode> field = fieldsIterator.next();
      properties.put(field.getKey(), field.getValue().asText());
    }
    return properties;
  }

  private static String convertToString(String property, JsonNode pNode) {
    Preconditions.checkArgument(
        pNode != null && !pNode.isNull() && pNode.isTextual(),
        "Cannot parse to a string value %s: %s",
        property,
        pNode);
    return pNode.asText();
  }

  /**
   * Write Gravitino Type to JSON node property. Usd for Gravity Type JSON serialization.
   *
   * @param dataType Gravitino Type
   * @param gen JSON generator used to write the type
   * @throws IOException if the type cannot be written
   */
  private static void writeDataType(Type dataType, JsonGenerator gen) throws IOException {
    switch (dataType.name()) {
      case BOOLEAN:
      case BYTE:
      case SHORT:
      case INTEGER:
      case LONG:
      case FLOAT:
      case DOUBLE:
      case DECIMAL:
      case DATE:
      case TIME:
      case TIMESTAMP:
      case STRING:
      case FIXEDCHAR:
      case VARCHAR:
      case INTERVAL_DAY:
      case INTERVAL_YEAR:
      case UUID:
      case FIXED:
      case BINARY:
      case NULL:
        // Primitive types and nullType are serialized as string
        gen.writeString(dataType.simpleString());
        break;
      case STRUCT:
        writeStructType((Types.StructType) dataType, gen);
        break;
      case LIST:
        writeListType((Types.ListType) dataType, gen);
        break;
      case MAP:
        writeMapType((Types.MapType) dataType, gen);
        break;
      case UNION:
        writeUnionType((Types.UnionType) dataType, gen);
        break;
      case UNPARSED:
        writeUnparsedType((Types.UnparsedType) dataType, gen);
        break;
      default:
        writeUnparsedType(dataType.simpleString(), gen);
    }
  }

  /**
   * Read Gravitino Type from JSON node property. Used for Gravity Type JSON deserialization.
   *
   * @param node JSON node containing the type
   * @return Gravitino Type
   */
  private static Type readDataType(JsonNode node) {
    Preconditions.checkArgument(
        node != null && !node.isNull(), "Cannot parse type from invalid JSON: %s", node);

    if (node.isTextual()) {
      String text = node.asText().toLowerCase();
      return text.equals(Types.NullType.get().simpleString())
          ? Types.NullType.get()
          : fromPrimitiveTypeString(text);
    }

    if (node.isObject() && node.has(TYPE)) {
      String type = node.get(TYPE).asText();

      if (STRUCT.equals(type)) {
        return readStructType(node);
      }

      if (LIST.equals(type)) {
        return readListType(node);
      }

      if (MAP.equals(type)) {
        return readMapType(node);
      }

      if (UNION.equals(type)) {
        return readUnionType(node);
      }

      if (UNPARSED.equals(type)) {
        return readUnparsedType(node);
      }
    }

    throw new IllegalArgumentException("Cannot parse type from JSON: " + node);
  }

  private static void writeUnionType(Types.UnionType unionType, JsonGenerator gen)
      throws IOException {
    gen.writeStartObject();

    gen.writeStringField(TYPE, UNION);
    gen.writeArrayFieldStart(UNION_TYPES);
    for (Type type : unionType.types()) {
      writeDataType(type, gen);
    }
    gen.writeEndArray();

    gen.writeEndObject();
  }

  private static void writeMapType(Types.MapType mapType, JsonGenerator gen) throws IOException {
    gen.writeStartObject();

    gen.writeStringField(TYPE, MAP);
    gen.writeBooleanField(MAP_VALUE_NULLABLE, mapType.valueNullable());
    gen.writeFieldName(MAP_KEY_TYPE);
    writeDataType(mapType.keyType(), gen);
    gen.writeFieldName(MAP_VALUE_TYPE);
    writeDataType(mapType.valueType(), gen);

    gen.writeEndObject();
  }

  private static void writeListType(Types.ListType listType, JsonGenerator gen) throws IOException {
    gen.writeStartObject();

    gen.writeStringField(TYPE, LIST);
    gen.writeBooleanField(LIST_ELEMENT_NULLABLE, listType.elementNullable());
    gen.writeFieldName(LIST_ELEMENT_TYPE);
    writeDataType(listType.elementType(), gen);

    gen.writeEndObject();
  }

  private static void writeStructType(Types.StructType structType, JsonGenerator gen)
      throws IOException {
    gen.writeStartObject();

    gen.writeStringField(TYPE, STRUCT);
    gen.writeArrayFieldStart(FIELDS);
    for (Types.StructType.Field field : structType.fields()) {
      writeStructField(field, gen);
    }
    gen.writeEndArray();

    gen.writeEndObject();
  }

  private static void writeStructField(Types.StructType.Field field, JsonGenerator gen)
      throws IOException {
    gen.writeStartObject();
    gen.writeStringField(STRUCT_FIELD_NAME, field.name());
    gen.writeFieldName(TYPE);
    writeDataType(field.type(), gen);
    gen.writeBooleanField(STRUCT_FIELD_NULLABLE, field.nullable());
    if (field.comment() != null) {
      gen.writeStringField(STRUCT_FIELD_COMMENT, field.comment());
    }
    gen.writeEndObject();
  }

  private static void writeUnparsedType(Types.UnparsedType unparsedType, JsonGenerator gen)
      throws IOException {
    writeUnparsedType(unparsedType.unparsedType(), gen);
  }

  private static void writeUnparsedType(String unparsedType, JsonGenerator gen) throws IOException {
    gen.writeStartObject();
    gen.writeStringField(TYPE, UNPARSED);
    gen.writeStringField(UNPARSED_TYPE, unparsedType);
    gen.writeEndObject();
  }

  private static Type.PrimitiveType fromPrimitiveTypeString(String typeString) {
    Type.PrimitiveType primitiveType = TYPES.get(typeString);
    if (primitiveType != null) {
      return primitiveType;
    }

    Matcher fixed = FIXED.matcher(typeString);
    if (fixed.matches()) {
      return Types.FixedType.of(Integer.parseInt(fixed.group(1)));
    }

    Matcher fixedChar = FIXEDCHAR.matcher(typeString);
    if (fixedChar.matches()) {
      return Types.FixedCharType.of(Integer.parseInt(fixedChar.group(1)));
    }

    Matcher varchar = VARCHAR.matcher(typeString);
    if (varchar.matches()) {
      return Types.VarCharType.of(Integer.parseInt(varchar.group(1)));
    }

    Matcher decimal = DECIMAL.matcher(typeString);
    if (decimal.matches()) {
      return Types.DecimalType.of(
          Integer.parseInt(decimal.group(1)), Integer.parseInt(decimal.group(2)));
    }

    throw new IllegalArgumentException("Cannot parse type string to primitiveType: " + typeString);
  }

  private static Types.StructType readStructType(JsonNode node) {
    JsonNode fields = node.get(FIELDS);
    Preconditions.checkArgument(
        fields != null && fields.isArray(),
        "Cannot parse struct fields from non-array: %s",
        fields);

    List<Types.StructType.Field> structFields = Lists.newArrayListWithExpectedSize(fields.size());
    for (JsonNode field : fields) {
      structFields.add(readStructField(field));
    }
    return Types.StructType.of(structFields.toArray(new Types.StructType.Field[0]));
  }

  private static Types.ListType readListType(JsonNode node) {
    Preconditions.checkArgument(
        node.has(LIST_ELEMENT_TYPE), "Cannot parse list type from missing element type: %s", node);

    Type elementType = readDataType(node.get(LIST_ELEMENT_TYPE));
    // use true as default value for nullable
    boolean nullable =
        node.has(LIST_ELEMENT_NULLABLE) ? node.get(LIST_ELEMENT_NULLABLE).asBoolean() : true;
    return Types.ListType.of(elementType, nullable);
  }

  private static Types.MapType readMapType(JsonNode node) {
    Preconditions.checkArgument(
        node.has(MAP_KEY_TYPE), "Cannot parse map type from missing key type: %s", node);
    Preconditions.checkArgument(
        node.has(MAP_VALUE_TYPE), "Cannot parse map type from missing value type: %s", node);

    Type keyType = readDataType(node.get(MAP_KEY_TYPE));
    Type valueType = readDataType(node.get(MAP_VALUE_TYPE));
    boolean nullable =
        node.has(MAP_VALUE_NULLABLE) ? node.get(MAP_VALUE_NULLABLE).asBoolean() : true;
    return Types.MapType.of(keyType, valueType, nullable);
  }

  private static Types.UnionType readUnionType(JsonNode node) {
    Preconditions.checkArgument(
        node.has(UNION_TYPES), "Cannot parse union type from missing types: %s", node);

    JsonNode types = node.get(UNION_TYPES);
    Preconditions.checkArgument(
        types != null && types.isArray(), "Cannot parse union types from non-array: %s", types);

    List<Type> unionTypes = Lists.newArrayListWithExpectedSize(types.size());
    for (JsonNode type : types) {
      unionTypes.add(readDataType(type));
    }
    return Types.UnionType.of(unionTypes.toArray(new Type[0]));
  }

  private static Types.StructType.Field readStructField(JsonNode node) {
    Preconditions.checkArgument(
        node != null && !node.isNull() && node.isObject(),
        "Cannot parse struct field from invalid JSON: %s",
        node);
    Preconditions.checkArgument(
        node.has(STRUCT_FIELD_NAME), "Cannot parse struct field from missing name: %s", node);
    Preconditions.checkArgument(
        node.has(TYPE), "Cannot parse struct field from missing type: %s", node);

    String name = getString(STRUCT_FIELD_NAME, node);
    Type type = readDataType(node.get(TYPE));
    // use true as default value for nullable
    boolean nullable =
        node.has(STRUCT_FIELD_NULLABLE) ? node.get(STRUCT_FIELD_NULLABLE).asBoolean() : true;
    String comment = node.has(STRUCT_FIELD_COMMENT) ? getString(STRUCT_FIELD_COMMENT, node) : null;
    return Types.StructType.Field.of(name, type, nullable, comment);
  }

  private static Types.UnparsedType readUnparsedType(JsonNode node) {
    Preconditions.checkArgument(
        node.has(UNPARSED_TYPE), "Cannot parse unparsed type from missing unparsed type: %s", node);

    return Types.UnparsedType.of(node.get(UNPARSED_TYPE).asText());
  }

  // Nested classes for custom serialization and deserialization

  /** Custom JSON serializer for Gravitino Type objects. */
  public static class TypeSerializer extends JsonSerializer<Type> {
    @Override
    public void serialize(Type value, JsonGenerator gen, SerializerProvider serializers)
        throws IOException {
      writeDataType(value, gen);
    }
  }

  /** Custom JSON deserializer for Gravitino Type objects. */
  public static class TypeDeserializer extends JsonDeserializer<Type> {

    @Override
    public Type deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
      return readDataType(p.getCodec().readTree(p));
    }
  }

  /** Custom JSON serializer for NameIdentifier objects. */
  public static class NameIdentifierSerializer extends JsonSerializer<NameIdentifier> {

    @Override
    public void serialize(NameIdentifier value, JsonGenerator gen, SerializerProvider serializers)
        throws IOException {
      gen.writeStartObject();
      gen.writeFieldName(NAMESPACE);
      gen.writeArray(value.namespace().levels(), 0, value.namespace().length());
      gen.writeStringField(NAME, value.name());
      gen.writeEndObject();
    }
  }

  /** Custom JSON deserializer for NameIdentifier objects. */
  public static class NameIdentifierDeserializer extends JsonDeserializer<NameIdentifier> {

    @Override
    public NameIdentifier deserialize(JsonParser p, DeserializationContext ctxt)
        throws IOException {
      JsonNode node = p.getCodec().readTree(p);
      Preconditions.checkArgument(
          node != null && !node.isNull() && node.isObject(),
          "Cannot parse name identifier from invalid JSON: %s",
          node);
      List<String> levels = getStringListOrNull(NAMESPACE, node);
      String name = getString(NAME, node);

      Namespace namespace =
          levels == null ? Namespace.empty() : Namespace.of(levels.toArray(new String[0]));
      return NameIdentifier.of(namespace, name);
    }
  }

  /** Custom JSON serializer for SortOrderDTO objects. */
  public static class ColumnPositionSerializer extends JsonSerializer<TableChange.ColumnPosition> {
    @Override
    public void serialize(
        TableChange.ColumnPosition value, JsonGenerator gen, SerializerProvider serializers)
        throws IOException {
      if (value instanceof TableChange.First) {
        gen.writeString(POSITION_FIRST);

      } else if (value instanceof TableChange.After) {
        gen.writeStartObject();
        TableChange.After after = (TableChange.After) value;
        gen.writeStringField(POSITION_AFTER, after.getColumn());
        gen.writeEndObject();

      } else if (value instanceof TableChange.Default) {
        gen.writeString(POSITION_DEFAULT);
      } else {
        throw new IOException("Unknown column position: " + value);
      }
    }
  }

  /** Custom JSON deserializer for ColumnPosition objects. */
  public static class ColumnPositionDeserializer
      extends JsonDeserializer<TableChange.ColumnPosition> {

    @Override
    public TableChange.ColumnPosition deserialize(JsonParser p, DeserializationContext ctxt)
        throws IOException {
      JsonNode node = p.getCodec().readTree(p);
      Preconditions.checkArgument(
          node != null && !node.isNull(),
          "Cannot parse column position from invalid JSON: %s",
          node);
      if (node.isTextual()
          && (node.asText().equals(POSITION_FIRST)
              || node.asText().equals(POSITION_FIRST.toUpperCase()))) {
        return TableChange.ColumnPosition.first();
      } else if (node.isTextual()
          && (node.asText().equalsIgnoreCase(POSITION_DEFAULT)
              || node.asText().equalsIgnoreCase(POSITION_DEFAULT.toUpperCase()))) {
        return TableChange.ColumnPosition.defaultPos();
      } else if (node.isObject()) {
        String afterColumn = getString(POSITION_AFTER, node);
        return TableChange.ColumnPosition.after(afterColumn);
      } else {
        throw new IOException("Unknown json column position: " + node);
      }
    }
  }

  /** Custom JSON serializer for PartitionDTO objects. */
  public static class PartitioningSerializer extends JsonSerializer<Partitioning> {
    @Override
    public void serialize(Partitioning value, JsonGenerator gen, SerializerProvider serializers)
        throws IOException {
      gen.writeStartObject();
      gen.writeStringField(STRATEGY, value.strategy().name().toLowerCase());
      switch (value.strategy()) {
        case IDENTITY:
        case YEAR:
        case MONTH:
        case DAY:
        case HOUR:
          String[] fieldName = ((Partitioning.SingleFieldPartitioning) value).fieldName();
          gen.writeFieldName(FIELD_NAME);
          gen.writeObject(fieldName);
          break;
        case BUCKET:
          BucketPartitioningDTO bucketPartitioningDTO = (BucketPartitioningDTO) value;
          gen.writeNumberField(NUM_BUCKETS, bucketPartitioningDTO.numBuckets());
          gen.writeFieldName(FIELD_NAMES);
          gen.writeObject(bucketPartitioningDTO.fieldNames());
          break;
        case TRUNCATE:
          TruncatePartitioningDTO truncatePartitioningDTO = (TruncatePartitioningDTO) value;
          gen.writeNumberField(WIDTH, truncatePartitioningDTO.width());
          gen.writeFieldName(FIELD_NAME);
          gen.writeObject(truncatePartitioningDTO.fieldName());
          break;
        case LIST:
          ListPartitioningDTO listPartitioningDTO = (ListPartitioningDTO) value;
          gen.writeFieldName(FIELD_NAMES);
          gen.writeObject(listPartitioningDTO.fieldNames());
          break;
        case RANGE:
          RangePartitioningDTO rangePartitioningDTO = (RangePartitioningDTO) value;
          gen.writeFieldName(FIELD_NAME);
          gen.writeObject(rangePartitioningDTO.fieldName());
          break;
        case FUNCTION:
          FunctionPartitioningDTO funcExpression = (FunctionPartitioningDTO) value;
          gen.writeStringField(FUNCTION_NAME, funcExpression.functionName());
          gen.writeArrayFieldStart(FUNCTION_ARGS);
          for (FunctionArg arg : funcExpression.args()) {
            writeFunctionArg(arg, gen);
          }
          gen.writeEndArray();
          break;
        default:
          throw new IOException("Unknown partitioning strategy: " + value.strategy());
      }
      gen.writeEndObject();
    }
  }

  /** Custom JSON deserializer for Partitioning objects. */
  public static class PartitioningDeserializer extends JsonDeserializer<Partitioning> {
    @Override
    public Partitioning deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
      JsonNode node = p.getCodec().readTree(p);
      Preconditions.checkArgument(
          node != null && !node.isNull() && node.isObject(),
          "Cannot parse partitioning from invalid JSON: %s",
          node);
      Preconditions.checkArgument(
          node.has(STRATEGY), "Cannot parse partitioning from missing strategy: %s", node);
      String strategy = getString(STRATEGY, node);
      switch (Partitioning.Strategy.getByName(strategy)) {
        case IDENTITY:
          return IdentityPartitioningDTO.of(getStringList(FIELD_NAME, node).toArray(new String[0]));
        case YEAR:
          return YearPartitioningDTO.of(getStringList(FIELD_NAME, node).toArray(new String[0]));
        case MONTH:
          return MonthPartitioningDTO.of(getStringList(FIELD_NAME, node).toArray(new String[0]));
        case DAY:
          return DayPartitioningDTO.of(getStringList(FIELD_NAME, node).toArray(new String[0]));
        case HOUR:
          return HourPartitioningDTO.of(getStringList(FIELD_NAME, node).toArray(new String[0]));
        case BUCKET:
          int numBuckets = getInt(NUM_BUCKETS, node);
          List<String[]> fieldNames = Lists.newArrayList();
          node.get(FIELD_NAMES).forEach(field -> fieldNames.add(getStringArray((ArrayNode) field)));
          return BucketPartitioningDTO.of(numBuckets, fieldNames.toArray(new String[0][0]));
        case TRUNCATE:
          int width = getInt(WIDTH, node);
          return TruncatePartitioningDTO.of(
              width, getStringList(FIELD_NAME, node).toArray(new String[0]));
        case LIST:
          List<String[]> listFields = Lists.newArrayList();
          node.get(FIELD_NAMES).forEach(field -> listFields.add(getStringArray((ArrayNode) field)));
          return ListPartitioningDTO.of(listFields.toArray(new String[0][0]));
        case RANGE:
          return RangePartitioningDTO.of(getStringList(FIELD_NAME, node).toArray(new String[0]));
        case FUNCTION:
          String functionName = getString(FUNCTION_NAME, node);
          Preconditions.checkArgument(
              node.has(FUNCTION_ARGS),
              "Cannot parse function partitioning from missing function args: %s",
              node);
          List<FunctionArg> args = Lists.newArrayList();
          node.get(FUNCTION_ARGS).forEach(arg -> args.add(readFunctionArg(arg)));
          return FunctionPartitioningDTO.of(functionName, args.toArray(FunctionArg.EMPTY_ARGS));
        default:
          throw new IOException("Unknown partitioning strategy: " + strategy);
      }
    }
  }

  /** Custom JSON serializer for SortOrderDTO objects. */
  public static class SortOrderSerializer extends JsonSerializer<SortOrderDTO> {
    @Override
    public void serialize(SortOrderDTO value, JsonGenerator gen, SerializerProvider serializers)
        throws IOException {
      gen.writeStartObject();
      gen.writeFieldName(SORT_TERM);
      writeFunctionArg(value.sortTerm(), gen);
      gen.writeStringField(DIRECTION, value.direction().toString());
      gen.writeStringField(NULL_ORDERING, value.nullOrdering().toString());
      gen.writeEndObject();
    }
  }

  /** Custom JSON deserializer for SortOrderDTO objects. */
  public static class SortOrderDeserializer extends JsonDeserializer<SortOrderDTO> {
    @Override
    public SortOrderDTO deserialize(JsonParser p, DeserializationContext ctxt)
        throws IOException, JacksonException {
      JsonNode node = p.getCodec().readTree(p);
      Preconditions.checkArgument(
          node != null && !node.isNull() && node.isObject(),
          "Cannot parse sort order from invalid JSON: %s",
          node);
      Preconditions.checkArgument(
          node.has(SORT_TERM), "Cannot parse sort order from missing sort term: %s", node);
      FunctionArg sortTerm = readFunctionArg(node.get(SORT_TERM));
      SortOrderDTO.Builder builder = new SortOrderDTO.Builder().withSortTerm(sortTerm);
      if (node.has(DIRECTION)) {
        builder.withDirection(SortDirection.fromString(getString(DIRECTION, node)));
      }
      if (node.has(NULL_ORDERING)) {
        builder.withNullOrder(NullOrdering.valueOf(getString(NULL_ORDERING, node).toUpperCase()));
      }
      return builder.build();
    }
  }

  /** Custom JSON serializer for DistributionDTO objects. */
  public static class DistributionSerializer extends JsonSerializer<DistributionDTO> {
    @Override
    public void serialize(DistributionDTO value, JsonGenerator gen, SerializerProvider serializers)
        throws IOException {
      gen.writeStartObject();
      gen.writeStringField(STRATEGY, value.strategy().name().toLowerCase());
      gen.writeNumberField(NUMBER, value.number());
      gen.writeArrayFieldStart(FUNCTION_ARGS);
      for (FunctionArg arg : value.args()) {
        writeFunctionArg(arg, gen);
      }
      gen.writeEndArray();
      gen.writeEndObject();
    }
  }

  /** Custom JSON deserializer for DistributionDTO objects. */
  public static class DistributionDeserializer extends JsonDeserializer<DistributionDTO> {
    @Override
    public DistributionDTO deserialize(JsonParser p, DeserializationContext ctxt)
        throws IOException {
      JsonNode node = p.getCodec().readTree(p);
      Preconditions.checkArgument(
          node != null && !node.isNull() && node.isObject(),
          "Cannot parse distribution from invalid JSON: %s",
          node);
      DistributionDTO.Builder builder = new DistributionDTO.Builder();
      if (node.has(STRATEGY)) {
        String strategy = getString(STRATEGY, node);
        builder.withStrategy(Strategy.getByName(strategy));
      }
      builder.withNumber(getInt(NUMBER, node));
      List<FunctionArg> args = Lists.newArrayList();
      node.get(FUNCTION_ARGS).forEach(arg -> args.add(readFunctionArg(arg)));
      return builder.withArgs(args.toArray(FunctionArg.EMPTY_ARGS)).build();
    }
  }

  /** Custom JSON serializer for Column default value. */
  public static class ColumnDefaultValueSerializer extends JsonSerializer<Expression> {
    @Override
    public void serialize(Expression value, JsonGenerator gen, SerializerProvider serializers)
        throws IOException {
      if (value == null || value.equals(Column.DEFAULT_VALUE_NOT_SET)) {
        return;
      }
      writeFunctionArg((FunctionArg) value, gen);
    }

    @Override
    public boolean isEmpty(SerializerProvider provider, Expression value) {
      return value == null || value.equals(Column.DEFAULT_VALUE_NOT_SET);
    }
  }

  /** Custom JSON deserializer for Column default value. */
  public static class ColumnDefaultValueDeserializer extends JsonDeserializer<Expression> {
    @Override
    public Expression deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
      JsonNode node = p.getCodec().readTree(p);
      if (node == null || node.isNull()) {
        return Column.DEFAULT_VALUE_NOT_SET;
      }
      return readFunctionArg(node);
    }
  }

  /** Custom JSON serializer for PartitionDTO objects. */
  public static class PartitionDTOSerializer extends JsonSerializer<PartitionDTO> {
    @Override
    public void serialize(PartitionDTO value, JsonGenerator gen, SerializerProvider serializers)
        throws IOException {
      gen.writeStartObject();
      gen.writeStringField(PARTITION_TYPE, value.type().name().toLowerCase());
      gen.writeStringField(PARTITION_NAME, value.name());
      switch (value.type()) {
        case IDENTITY:
          IdentityPartitionDTO identityPartitionDTO = (IdentityPartitionDTO) value;
          gen.writeFieldName(FIELD_NAMES);
          gen.writeObject(identityPartitionDTO.fieldNames());
          gen.writeArrayFieldStart(IDENTITY_PARTITION_VALUES);
          for (LiteralDTO literal : identityPartitionDTO.values()) {
            writeFunctionArg(literal, gen);
          }
          gen.writeEndArray();
          break;
        case LIST:
          ListPartitionDTO listPartitionDTO = (ListPartitionDTO) value;
          gen.writeArrayFieldStart(LIST_PARTITION_LISTS);
          for (LiteralDTO[] literals : listPartitionDTO.lists()) {
            gen.writeStartArray();
            for (LiteralDTO literal : literals) {
              writeFunctionArg(literal, gen);
            }
            gen.writeEndArray();
          }
          gen.writeEndArray();
          break;
        case RANGE:
          RangePartitionDTO rangePartitionDTO = (RangePartitionDTO) value;
          gen.writeFieldName(RANGE_PARTITION_UPPER);
          writeFunctionArg(rangePartitionDTO.upper(), gen);
          gen.writeFieldName(RANGE_PARTITION_LOWER);
          writeFunctionArg(rangePartitionDTO.lower(), gen);
          break;
        default:
          throw new IOException("Unknown partition type: " + value.type());
      }
      gen.writeObjectField(PARTITION_PROPERTIES, value.properties());
      gen.writeEndObject();
    }
  }

  /** Custom JSON deserializer for PartitionDTO objects. */
  public static class PartitionDTODeserializer extends JsonDeserializer<PartitionDTO> {
    @Override
    public PartitionDTO deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
      JsonNode node = p.getCodec().readTree(p);
      Preconditions.checkArgument(
          node != null && !node.isNull() && node.isObject(),
          "Partition must be a valid JSON object, but found: %s",
          node);
      Preconditions.checkArgument(
          node.has(PARTITION_TYPE), "Partition must have a type field, but found: %s", node);
      String type = getString(PARTITION_TYPE, node);
      switch (PartitionDTO.Type.valueOf(type.toUpperCase())) {
        case IDENTITY:
          Preconditions.checkArgument(
              node.has(FIELD_NAMES) && node.get(FIELD_NAMES).isArray(),
              "Identity partition must have array of fieldNames, but found: %s",
              node);
          Preconditions.checkArgument(
              node.has(IDENTITY_PARTITION_VALUES) && node.get(IDENTITY_PARTITION_VALUES).isArray(),
              "Identity partition must have array of values, but found: %s",
              node);

          List<String[]> fieldNames = Lists.newArrayList();
          node.get(FIELD_NAMES).forEach(field -> fieldNames.add(getStringArray((ArrayNode) field)));
          List<LiteralDTO> values = Lists.newArrayList();
          node.get(IDENTITY_PARTITION_VALUES)
              .forEach(value -> values.add((LiteralDTO) readFunctionArg(value)));
          return IdentityPartitionDTO.builder()
              .withName(getStringOrNull(PARTITION_NAME, node))
              .withFieldNames(fieldNames.toArray(new String[0][0]))
              .withValues(values.toArray(new LiteralDTO[0]))
              .withProperties(getStringMapOrNull(PARTITION_PROPERTIES, node))
              .build();

        case LIST:
          Preconditions.checkArgument(
              node.has(PARTITION_NAME), "List partition must have name, but found: %s", node);
          Preconditions.checkArgument(
              node.has(LIST_PARTITION_LISTS) && node.get(LIST_PARTITION_LISTS).isArray(),
              "List partition must have array of lists, but found: %s",
              node);

          List<LiteralDTO[]> lists = Lists.newArrayList();
          node.get(LIST_PARTITION_LISTS)
              .forEach(
                  list -> {
                    List<LiteralDTO> literals = Lists.newArrayList();
                    list.forEach(literal -> literals.add((LiteralDTO) readFunctionArg(literal)));
                    lists.add(literals.toArray(new LiteralDTO[0]));
                  });

          return ListPartitionDTO.builder()
              .withName(getStringOrNull(PARTITION_NAME, node))
              .withLists(lists.toArray(new LiteralDTO[0][0]))
              .withProperties(getStringMapOrNull(PARTITION_PROPERTIES, node))
              .build();

        case RANGE:
          Preconditions.checkArgument(
              node.has(PARTITION_NAME), "Range partition must have name, but found: %s", node);
          Preconditions.checkArgument(
              node.has(RANGE_PARTITION_UPPER),
              "Range partition must have upper, but found: %s",
              node);
          Preconditions.checkArgument(
              node.has(RANGE_PARTITION_LOWER),
              "Range partition must have lower, but found: %s",
              node);

          LiteralDTO upper = (LiteralDTO) readFunctionArg(node.get(RANGE_PARTITION_UPPER));
          LiteralDTO lower = (LiteralDTO) readFunctionArg(node.get(RANGE_PARTITION_LOWER));
          return RangePartitionDTO.builder()
              .withName(getStringOrNull(PARTITION_NAME, node))
              .withUpper(upper)
              .withLower(lower)
              .withProperties(getStringMapOrNull(PARTITION_PROPERTIES, node))
              .build();

        default:
          throw new IOException("Unknown partition type: " + type);
      }
    }
  }

  /** Custom JSON serializer for Index objects. */
  public static class IndexSerializer extends JsonSerializer<Index> {
    @Override
    public void serialize(Index value, JsonGenerator gen, SerializerProvider serializers)
        throws IOException {
      gen.writeStartObject();
      gen.writeStringField(INDEX_TYPE, value.type().name().toUpperCase(Locale.ROOT));
      if (null != value.name()) {
        gen.writeStringField(INDEX_NAME, value.name());
      }
      gen.writeFieldName(INDEX_FIELD_NAMES);
      gen.writeObject(value.fieldNames());
      gen.writeEndObject();
    }
  }

  /** Custom JSON deserializer for Index objects. */
  public static class IndexDeserializer extends JsonDeserializer<Index> {
    @Override
    public Index deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
      JsonNode node = p.getCodec().readTree(p);
      Preconditions.checkArgument(
          node != null && !node.isNull() && node.isObject(),
          "Index must be a valid JSON object, but found: %s",
          node);

      IndexDTO.Builder builder = new IndexDTO.Builder();
      Preconditions.checkArgument(
          node.has(INDEX_TYPE), "Cannot parse index from missing type: %s", node);
      String indexType = getString(INDEX_TYPE, node);
      builder.withIndexType(Index.IndexType.valueOf(indexType.toUpperCase(Locale.ROOT)));
      if (node.has(INDEX_NAME)) {
        builder.withName(getString(INDEX_NAME, node));
      }
      Preconditions.checkArgument(
          node.has(INDEX_FIELD_NAMES), "Cannot parse index from missing field names: %s", node);
      List<String[]> fieldNames = Lists.newArrayList();
      node.get(INDEX_FIELD_NAMES)
          .forEach(field -> fieldNames.add(getStringArray((ArrayNode) field)));
      builder.withFieldNames(fieldNames.toArray(new String[0][0]));
      return builder.build();
    }
  }

  private JsonUtils() {}
}
