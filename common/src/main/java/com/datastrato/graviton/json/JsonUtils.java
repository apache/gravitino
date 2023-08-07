/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.json;

import com.datastrato.graviton.NameIdentifier;
import com.datastrato.graviton.Namespace;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
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

  private static ObjectMapper mapper = null;

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
                  .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
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

    return ImmutableList.<String>builder()
        .addAll(new JsonStringArrayIterator(property, node))
        .build();
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
}
