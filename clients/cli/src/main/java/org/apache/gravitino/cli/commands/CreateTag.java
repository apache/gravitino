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

package org.apache.gravitino.cli.commands;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.gravitino.cli.ErrorMessages;
import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.exceptions.NoSuchMetalakeException;
import org.apache.gravitino.exceptions.TagAlreadyExistsException;

public class CreateTag extends Command {
  protected final String metalake;
  protected final String[] tags;
  protected final String comment;

  /**
   * Create tags.
   *
   * @param url The URL of the Gravitino server.
   * @param ignoreVersions If true don't check the client/server versions match.
   * @param metalake The name of the metalake.
   * @param tags The names of the tags.
   * @param comment The comment of the tag.
   */
  public CreateTag(
      String url, boolean ignoreVersions, String metalake, String[] tags, String comment) {
    super(url, ignoreVersions);
    this.metalake = metalake;
    this.tags = tags;
    this.comment = comment;
  }

  /** Create tags. */
  @Override
  public void handle() {
    if (tags == null || tags.length == 0) {
      System.err.println(ErrorMessages.TAG_EMPTY);
    } else {
      boolean hasOnlyOneTag = tags.length == 1;
      if (hasOnlyOneTag) {
        handleOnlyOneTag();
      } else {
        handleMultipleTags();
      }
    }
  }

  private void handleOnlyOneTag() {
    try {
      GravitinoClient client = buildClient(metalake);
      client.createTag(tags[0], comment, null);
    } catch (NoSuchMetalakeException err) {
      System.err.println(ErrorMessages.UNKNOWN_METALAKE);
      return;
    } catch (TagAlreadyExistsException err) {
      System.err.println(ErrorMessages.TAG_EXISTS);
      return;
    } catch (Exception exp) {
      System.err.println(exp.getMessage());
      return;
    }

    System.out.println("Tag " + tags[0] + " created");
  }

  private void handleMultipleTags() {
    List<String> created = new ArrayList<>();
    try {
      GravitinoClient client = buildClient(metalake);
      for (String tag : tags) {
        client.createTag(tag, comment, null);
        created.add(tag);
      }
    } catch (NoSuchMetalakeException err) {
      System.err.println(ErrorMessages.UNKNOWN_METALAKE);
      return;
    } catch (TagAlreadyExistsException err) {
      System.err.println(ErrorMessages.TAG_EXISTS);
      return;
    } catch (Exception exp) {
      System.err.println(exp.getMessage());
      return;
    }
    if (!created.isEmpty()) {
      System.out.println("Tags " + String.join(",", created) + " created");
    }
    if (created.size() < tags.length) {
      List<String> remaining = Arrays.asList(tags);
      remaining.removeAll(created);
      System.out.println("Tags " + String.join(",", remaining) + " not created");
    }
  }
}
