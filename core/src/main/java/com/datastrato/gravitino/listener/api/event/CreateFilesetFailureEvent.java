/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.listener.api.event;

import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.listener.api.info.FilesetInfo;

/** Represents an event triggered upon the unsuccessful attempt to create a fileset. */
public final class CreateFilesetFailureEvent extends FilesetFailureEvent {
  private final FilesetInfo createFilesetRequest;

  /**
   * Constructs a new {@code CreateFilesetFailureEvent}, capturing the specifics of the failed
   * fileset creation attempt.
   *
   * @param user The user who initiated the attempt to create the fileset.
   * @param identifier The identifier of the fileset intended for creation.
   * @param exception The exception encountered during the fileset creation process, shedding light
   *     on the potential reasons behind the failure.
   * @param createFilesetRequest The original request information used to attempt to create the
   *     fileset.
   */
  public CreateFilesetFailureEvent(
      String user,
      NameIdentifier identifier,
      Exception exception,
      FilesetInfo createFilesetRequest) {
    super(user, identifier, exception);
    this.createFilesetRequest = createFilesetRequest;
  }

  /**
   * Provides insight into the intended configuration of the fileset at the time of the failed
   * creation attempt.
   *
   * @return The {@link FilesetInfo} instance representing the request information for the failed
   *     fileset creation attempt.
   */
  public FilesetInfo createFilesetRequest() {
    return createFilesetRequest;
  }
}
