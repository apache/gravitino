package org.apache.gravitino.listener.api.info;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.gravitino.annotation.DeveloperApi;

/**
 * Provides access to metadata about a Tag instance, designed for use by event listeners. This class
 * encapsulates the essential attributes of a Tag, including its name, optional description,
 * properties, and audit information.
 */
@DeveloperApi
public final class TagInfo {
  private final String name;
  @Nullable private final String comment;
  private final Map<String, String> properties;

  /**
   * Directly constructs TagInfo with specified details.
   *
   * @param name The name of the Tag.
   * @param comment An optional description for the Tag.
   * @param properties A map of properties associated with the Tag.
   */
  public TagInfo(String name, String comment, Map<String, String> properties) {
    this.name = name;
    this.comment = comment;
    this.properties = properties == null ? ImmutableMap.of() : ImmutableMap.copyOf(properties);
  }

  /**
   * Returns the name of the Tag.
   *
   * @return The Tag's name.
   */
  public String name() {
    return name;
  }

  /**
   * Returns the optional comment describing the Tag.
   *
   * @return The comment, or null if not provided.
   */
  @Nullable
  public String comment() {
    return comment;
  }

  /**
   * Returns the properties of the Tag.
   *
   * @return A map of Tag properties.
   */
  public Map<String, String> properties() {
    return properties;
  }
}
