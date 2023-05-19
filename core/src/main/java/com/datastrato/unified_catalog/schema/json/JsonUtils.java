package com.datastrato.unified_catalog.schema.json;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.substrait.type.StringTypeVisitor;
import io.substrait.type.Type;
import io.substrait.type.parser.ParseToPojo;
import io.substrait.type.parser.TypeStringParser;
import java.io.IOException;

public class JsonUtils {
  private static ObjectMapper mapper = null;

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

  public static class TypeSerializer extends JsonSerializer<io.substrait.type.Type> {
    private final StringTypeVisitor visitor = new StringTypeVisitor();

    @Override
    public void serialize(Type value, JsonGenerator gen, SerializerProvider serializers)
        throws IOException {
      try {
        gen.writeString(value.accept(visitor));
      } catch (Exception e) {
        throw new IOException("Unable to serialize type " + value, e);
      }
    }
  }

  public static class TypeDeserializer extends JsonDeserializer<io.substrait.type.Type> {

    @Override
    public Type deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
      String s = p.getValueAsString();
      try {
        return TypeStringParser.parse(s, ParseToPojo::type);
      } catch (Exception e) {
        throw new IOException("Unable to parse string " + s.replace("\n", " \\n"), e);
      }
    }
  }
}
