/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.json;

import static com.datastrato.gravitino.dto.rel.expressions.FunctionArg.ArgType.FUNCTION;

import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.dto.rel.DistributionDTO;
import com.datastrato.gravitino.dto.rel.SortOrderDTO;
import com.datastrato.gravitino.dto.rel.expressions.FieldReferenceDTO;
import com.datastrato.gravitino.dto.rel.expressions.FuncExpressionDTO;
import com.datastrato.gravitino.dto.rel.expressions.FunctionArg;
import com.datastrato.gravitino.dto.rel.expressions.LiteralDTO;
import com.datastrato.gravitino.dto.rel.partitions.BucketPartitioningDTO;
import com.datastrato.gravitino.dto.rel.partitions.DayPartitioningDTO;
import com.datastrato.gravitino.dto.rel.partitions.FunctionPartitioningDTO;
import com.datastrato.gravitino.dto.rel.partitions.HourPartitioningDTO;
import com.datastrato.gravitino.dto.rel.partitions.IdentityPartitioningDTO;
import com.datastrato.gravitino.dto.rel.partitions.ListPartitioningDTO;
import com.datastrato.gravitino.dto.rel.partitions.MonthPartitioningDTO;
import com.datastrato.gravitino.dto.rel.partitions.Partitioning;
import com.datastrato.gravitino.dto.rel.partitions.RangePartitioningDTO;
import com.datastrato.gravitino.dto.rel.partitions.TruncatePartitioningDTO;
import com.datastrato.gravitino.dto.rel.partitions.YearPartitioningDTO;
import com.datastrato.gravitino.rel.TableChange;
import com.datastrato.gravitino.rel.expressions.distributions.Strategy;
import com.datastrato.gravitino.rel.expressions.sorts.NullOrdering;
import com.datastrato.gravitino.rel.expressions.sorts.SortDirection;
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
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import io.substrait.type.StringTypeVisitor;
import io.substrait.type.Type;
import io.substrait.type.parser.ParseToPojo;
import io.substrait.type.parser.TypeStringParser;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Utility class for working with JSON data. */
public class JsonUtils {

  private static final Logger LOG = LoggerFactory.getLogger(JsonUtils.class);
  private static final String NAMESPACE = "namespace";
  private static final String NAME = "name";
  private static final String POSITION_FIRST = "first";
  private static final String POSITION_LAST = "last";
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
  private static final String SORT_TERM = "sortTerm";
  private static final String DIRECTION = "direction";
  private static final String NULL_ORDERING = "nullOrdering";
  private static final String NUMBER = "number";

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

    @Override
    public boolean hasNext() {
      return elements.hasNext();
    }

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

  private static volatile ObjectMapper mapper = null;

  /**
   * Get the shared ObjectMapper instance for JSON serialization/deserialization.
   *
   * @return The ObjectMapper instance.
   */
  public static ObjectMapper objectMapper() {
    if (mapper == null) {
      synchronized (JsonUtils.class) {
        if (mapper == null) {
          mapper =
              new ObjectMapper()
                  .registerModule(new JavaTimeModule())
                  .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false)
                  .configure(EnumFeature.WRITE_ENUMS_TO_LOWERCASE, true)
                  .enable(MapperFeature.ACCEPT_CASE_INSENSITIVE_ENUMS);
        }
      }
    }

    return mapper;
  }

  /**
   * Get a list of strings from a JSON node property.
   *
   * @param property The property name.
   * @param node The JSON node.
   * @return The list of strings or null if property is missing or null.
   */
  public static List<String> getStringListOrNull(String property, JsonNode node) {
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
  public static List<String> getStringList(String property, JsonNode node) {
    Preconditions.checkArgument(node.has(property), "Cannot parse missing property: %s", property);
    return ImmutableList.<String>builder()
        .addAll(new JsonStringArrayIterator(property, node))
        .build();
  }

  public static String[] getStringArray(ArrayNode node) {
    String[] array = new String[node.size()];
    for (int i = 0; i < node.size(); i++) {
      array[i] = node.get(i).asText();
    }
    return array;
  }

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

  public static FunctionArg readFunctionArg(JsonNode node) {
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
        Type dataType = TypeStringParser.parse(getString(DATA_TYPE, node), ParseToPojo::type);
        String value = getString(LITERAL_VALUE, node);
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
      default:
        throw new IllegalArgumentException("Unknown function argument type: " + type);
    }
  }

  public static void writeFunctionArg(FunctionArg arg, JsonGenerator gen) throws IOException {
    gen.writeStartObject();
    gen.writeStringField(EXPRESSION_TYPE, arg.argType().name().toLowerCase());
    switch (arg.argType()) {
      case LITERAL:
        gen.writeStringField(
            DATA_TYPE, ((LiteralDTO) arg).dataType().accept(new StringTypeVisitor()));
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

  private static String convertToString(String property, JsonNode pNode) {
    Preconditions.checkArgument(
        pNode != null && !pNode.isNull() && pNode.isTextual(),
        "Cannot parse to a string value %s: %s",
        property,
        pNode);
    return pNode.asText();
  }

  // Nested classes for custom serialization and deserialization

  /** Custom JSON serializer for Substrait Type objects. */
  public static class TypeSerializer extends JsonSerializer<io.substrait.type.Type> {

    private final StringTypeVisitor visitor = new StringTypeVisitor();

    @Override
    public void serialize(Type value, JsonGenerator gen, SerializerProvider serializers)
        throws IOException {
      try {
        gen.writeString(value.accept(visitor));
      } catch (Exception e) {
        LOG.warn("Unable to serialize type {}.", value, e);
        throw new IOException("Unable to serialize type " + value, e);
      }
    }
  }

  /** Custom JSON deserializer for Substrait Type objects. */
  public static class TypeDeserializer extends JsonDeserializer<io.substrait.type.Type> {

    @Override
    public Type deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
      String s = p.getValueAsString();
      try {
        return TypeStringParser.parse(s, ParseToPojo::type);
      } catch (Exception e) {
        LOG.warn("Unable to parse string {}.", s.replace("\n", " \\n"), e);
        throw new IOException("Unable to parse string " + s.replace("\n", " \\n"), e);
      }
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
        gen.writeStringField(POSITION_LAST, after.getColumn());
        gen.writeEndObject();

      } else {
        throw new IOException("Unknown column position: " + value);
      }
    }
  }

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
      if (node.isTextual() && node.asText().equals(POSITION_FIRST)) {
        return TableChange.ColumnPosition.first();
      } else if (node.isObject()) {
        String afterColumn = getString(POSITION_LAST, node);
        return TableChange.ColumnPosition.after(afterColumn);
      } else {
        throw new IOException("Unknown json column position: " + node);
      }
    }
  }

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
      switch (Partitioning.Strategy.fromString(strategy)) {
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
        builder.withStrategy(Strategy.fromString(strategy));
      }
      builder.withNumber(getInt(NUMBER, node));
      List<FunctionArg> args = Lists.newArrayList();
      node.get(FUNCTION_ARGS).forEach(arg -> args.add(readFunctionArg(arg)));
      return builder.withArgs(args.toArray(FunctionArg.EMPTY_ARGS)).build();
    }
  }
}
