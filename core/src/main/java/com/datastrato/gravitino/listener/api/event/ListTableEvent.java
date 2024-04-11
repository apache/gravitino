/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.listener.api.event;

import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.annotation.DeveloperApi;

/**
 * Represents an event that is triggered upon the successful enumeration of tables within a
 * namespace. This class extends {@link TableEvent} to provide specific information related to the
 * enumeration process, including details about the namespace from which the tables were listed.
 *
 * <p>To optimize memory usage and avoid the potential overhead associated with storing a large
 * number of tables directly within the ListTableEvent, the actual tables listed are not maintained
 * in this event. This design decision helps in managing resource efficiency, especially in
 * environments with extensive table listings.
 */
@DeveloperApi
public final class ListTableEvent extends TableEvent {
  private final Namespace namespace;

  /**
   * Constructs an instance of {@code ListTableEvent}, capturing key details about the successful
   * enumeration of tables within a namespace.
   *
   * <p>This constructor signifies the completion of the table listing process, documenting the
   * specific namespace whose tables were enumerated. This can aid in tracking the context of
   * operations and for generating comprehensive audit trails.
   *
   * @param user The username of the individual who initiated the table listing. This information is
   *     essential for identifying the source of operations and for auditing purposes.
   * @param namespace The namespace from which tables were listed, providing insight into the area
   *     of the system where the operation took place.
   */
  public ListTableEvent(String user, Namespace namespace) {
    super(user, NameIdentifier.parse(namespace.toString()));
    this.namespace = namespace;
  }

  /**
   * Provides the namespace associated with this event, reflecting the context within which the
   * tables were enumerated.
   *
   * @return A {@link Namespace} instance that encapsulates the details of the namespace, offering
   *     insights into the organization and scope of tables listed during the event.
   */
  public Namespace namespace() {
    return namespace;
  }
}
