/*·Copyright·2023·Datastrato.·This·software·is·licensed·under·the·Apache·License·version·2.·*/
package com.datastrato.graviton;

import com.datastrato.graviton.rel.Column;
import io.substrait.type.Type;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;
import lombok.EqualsAndHashCode;
import lombok.ToString;

@EqualsAndHashCode
@ToString
public class TestColumn implements Column, Entity, HasIdentifier {

  public static final Field NAME = Field.required("name", String.class, "The name of the column");
  public static final Field COMMENT =
      Field.optional("comment", String.class, "The comment of the column");
  public static final Field TYPE = Field.required("type", Type.class, "The type of the column");

  private String name;

  private String comment;

  private Type dataType;

  public TestColumn(String name, String comment, Type dataType) {
    this.name = name;
    this.comment = comment;
    this.dataType = dataType;

    validate();
  }

  // For Jackson Deserialization only.
  public TestColumn() {}

  @Override
  public Map<Field, Object> fields() {
    Map<Field, Object> fields = new HashMap<>();
    fields.put(NAME, name);
    fields.put(COMMENT, comment);
    fields.put(TYPE, dataType);

    return Collections.unmodifiableMap(fields);
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public Type dataType() {
    return dataType;
  }

  @Nullable
  @Override
  public String comment() {
    return comment;
  }
}
