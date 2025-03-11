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

import org.apache.gravitino.cli.CommandContext;
import org.apache.gravitino.cli.ErrorMessages;
import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.exceptions.NoSuchMetalakeException;
import org.apache.gravitino.exceptions.NoSuchTagException;
import org.apache.gravitino.tag.TagChange;

/** Set a property of a tag. */
public class SetTagProperty extends Command {

  protected final String metalake;
  protected final String tag;
  protected final String property;
  protected final String value;

  /**
   * Set a property of a tag.
   *
   * @param context The command context.
   * @param metalake The name of the metalake.
   * @param tag The name of the tag.
   * @param property The name of the property.
   * @param value The value of the property.
   */
  public SetTagProperty(
      CommandContext context, String metalake, String tag, String property, String value) {
    super(context);
    this.metalake = metalake;
    this.tag = tag;
    this.property = property;
    this.value = value;
  }

  /** Set a property of a tag. */
  @Override
  public void handle() {
    try {
      GravitinoClient client = buildClient(metalake);
      TagChange change = TagChange.setProperty(property, value);
      client.alterTag(tag, change);
    } catch (NoSuchMetalakeException err) {
      exitWithError(ErrorMessages.UNKNOWN_METALAKE);
    } catch (NoSuchTagException err) {
      exitWithError(ErrorMessages.UNKNOWN_TAG);
    } catch (Exception exp) {
      exitWithError(exp.getMessage());
    }

    printInformation(tag + " property set.");
  }

  @Override
  public Command validate() {
    validatePropertyAndValue(property, value);
    return super.validate();
  }
}
