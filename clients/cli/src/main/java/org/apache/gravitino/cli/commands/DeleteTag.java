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
import org.apache.gravitino.cli.AreYouSure;
import org.apache.gravitino.cli.CommandContext;
import org.apache.gravitino.cli.ErrorMessages;
import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.exceptions.NoSuchMetalakeException;
import org.apache.gravitino.exceptions.NoSuchTagException;

public class DeleteTag extends Command {

  protected final String metalake;
  protected final String[] tags;
  protected final boolean force;

  /**
   * Delete tags.
   *
   * @param context The command context.
   * @param metalake The name of the metalake.
   * @param tags The names of the tags.
   */
  public DeleteTag(CommandContext context, String metalake, String[] tags) {
    super(context);
    this.force = context.force();
    this.metalake = metalake;
    this.tags = tags;
  }

  /** Delete tags. */
  @Override
  public void handle() {
    if (!AreYouSure.really(force)) {
      return;
    }

    if (tags == null || tags.length == 0) {
      exitWithError(ErrorMessages.MISSING_TAG);
    } else {
      boolean hasOnlyOneTag = tags.length == 1;
      if (hasOnlyOneTag) {
        handleOnlyOneTag();
      } else {
        handleMultipleTags();
      }
    }
  }

  private void handleMultipleTags() {
    List<String> deleted = new ArrayList<>();
    try {
      GravitinoClient client = buildClient(metalake);
      for (String tag : tags) {
        if (client.deleteTag(tag)) {
          deleted.add(tag);
        }
      }
    } catch (NoSuchMetalakeException err) {
      exitWithError(ErrorMessages.UNKNOWN_METALAKE);
    } catch (NoSuchTagException err) {
      exitWithError(ErrorMessages.UNKNOWN_TAG);
    } catch (Exception exp) {
      exitWithError(exp.getMessage());
    }
    if (!deleted.isEmpty()) {
      printInformation("Tags " + String.join(",", deleted) + " deleted.");
    }
    if (deleted.size() < tags.length) {
      List<String> remaining = Arrays.asList(tags);
      remaining.removeAll(deleted);
      printInformation("Tags " + String.join(",", remaining) + " not deleted.");
    }
  }

  private void handleOnlyOneTag() {
    boolean deleted = false;

    try {
      GravitinoClient client = buildClient(metalake);
      deleted = client.deleteTag(tags[0]);
    } catch (NoSuchMetalakeException err) {
      exitWithError(ErrorMessages.UNKNOWN_METALAKE);
    } catch (NoSuchTagException err) {
      exitWithError(ErrorMessages.UNKNOWN_TAG);
    } catch (Exception exp) {
      exitWithError(exp.getMessage());
    }

    if (deleted) {
      printInformation("Tag " + tags[0] + " deleted.");
    } else {
      printInformation("Tag " + tags[0] + " not deleted.");
    }
  }

  @Override
  public Command validate() {
    if (tags == null) exitWithError(ErrorMessages.MISSING_TAG);
    return super.validate();
  }
}
