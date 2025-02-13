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

import static org.apache.gravitino.client.GravitinoClientBase.Builder;

import com.google.common.base.Joiner;
import java.io.File;
import org.apache.gravitino.cli.CommandContext;
import org.apache.gravitino.cli.ErrorMessages;
import org.apache.gravitino.cli.GravitinoConfig;
import org.apache.gravitino.cli.KerberosData;
import org.apache.gravitino.cli.Main;
import org.apache.gravitino.cli.OAuthData;
import org.apache.gravitino.cli.outputs.PlainFormat;
import org.apache.gravitino.cli.outputs.TableFormat;
import org.apache.gravitino.client.DefaultOAuth2TokenProvider;
import org.apache.gravitino.client.GravitinoAdminClient;
import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.client.GravitinoClientBase;
import org.apache.gravitino.client.KerberosTokenProvider;
import org.apache.gravitino.exceptions.NoSuchMetalakeException;

/* The base for all commands. */
public abstract class Command {
  public static final String OUTPUT_FORMAT_TABLE = "table";
  public static final String OUTPUT_FORMAT_PLAIN = "plain";
  public static final Joiner COMMA_JOINER = Joiner.on(", ").skipNulls();

  protected static String authentication = null;
  protected static String userName = null;

  private static final String SIMPLE_AUTH = "simple";
  private static final String OAUTH_AUTH = "oauth";
  private static final String KERBEROS_AUTH = "kerberos";

  private final String url;
  private final boolean ignoreVersions;
  private final String outputFormat;

  protected final CommandContext context;

  /**
   * Command constructor.
   *
   * @param context The command context.
   */
  public Command(CommandContext context) {
    this.context = context;
    this.url = context.url();
    this.ignoreVersions = context.ignoreVersions();
    this.outputFormat = context.outputFormat();
  }

  /**
   * Prints an error message and exits with a non-zero status.
   *
   * @param error The error message to display before exiting.
   */
  public void exitWithError(String error) {
    System.err.println(error);
    Main.exit(-1);
  }

  /**
   * Prints out an informational message, often to indicate a command has finished.
   *
   * @param message The message to display.
   */
  public void printInformation(String message) {
    if (context.quiet()) {
      return;
    }

    System.out.print(message);
  }

  /**
   * Prints out an a results of a command.
   *
   * @param results The results to display.
   */
  public void printResults(String results) {
    System.out.print(results);
  }

  /**
   * Sets the authentication mode and user credentials for the command.
   *
   * @param authentication the authentication mode to be used (e.g. "simple")
   * @param userName the username associated with the authentication mode
   */
  public static void setAuthenticationMode(String authentication, String userName) {
    Command.authentication = authentication;
    Command.userName = userName;
  }

  /** All commands have a handle method to handle and run the required command. */
  public abstract void handle();

  /**
   * verify the arguments. All commands have a verify method to verify the arguments.
   *
   * @return Returns itself via argument validation, otherwise exits.
   */
  public Command validate() {
    return this;
  }

  /**
   * Validates that both property and value arguments are not null.
   *
   * @param property The property name to check
   * @param value The value associated with the property
   */
  protected void validatePropertyAndValue(String property, String value) {
    if (property == null && value == null) exitWithError(ErrorMessages.MISSING_PROPERTY_AND_VALUE);
    if (property == null) exitWithError(ErrorMessages.MISSING_PROPERTY);
    if (value == null) exitWithError(ErrorMessages.MISSING_VALUE);
  }

  /**
   * Validates that the property argument is not null.
   *
   * @param property The property name to validate
   */
  protected void validateProperty(String property) {
    if (property == null) exitWithError(ErrorMessages.MISSING_PROPERTY);
  }

  /**
   * Builds a {@link GravitinoClient} instance with the provided server URL and metalake.
   *
   * @param metalake The name of the metalake.
   * @return A configured {@link GravitinoClient} instance.
   * @throws NoSuchMetalakeException if the specified metalake does not exist.
   */
  protected GravitinoClient buildClient(String metalake) throws NoSuchMetalakeException {
    Builder<GravitinoClient> client = GravitinoClient.builder(url).withMetalake(metalake);

    return constructClient(client).build();
  }

  /**
   * Builds a {@link GravitinoAdminClient} instance with the server URL.
   *
   * @return A configured {@link GravitinoAdminClient} instance.
   */
  protected GravitinoAdminClient buildAdminClient() {
    Builder<GravitinoAdminClient> client = GravitinoAdminClient.builder(url);

    return constructClient(client).build();
  }

  /**
   * Configures and constructs a {@link Builder} instance for creating a {@link GravitinoClient} or
   * {@link GravitinoAdminClient}.
   *
   * @param builder The {@link Builder} instance to be configured.
   * @param <T> The type of the {@link GravitinoClientBase}.
   * @return A configured {@link Builder} instance.
   */
  protected <T extends GravitinoClientBase> Builder<T> constructClient(Builder<T> builder) {
    if (ignoreVersions) {
      builder = builder.withVersionCheckDisabled();
    }
    if (authentication != null) {
      if (authentication.equals(SIMPLE_AUTH)) {
        if (userName != null && !userName.isEmpty()) {
          builder = builder.withSimpleAuth(userName);
        } else {
          builder = builder.withSimpleAuth();
        }
      } else if (authentication.equals(OAUTH_AUTH)) {
        GravitinoConfig config = new GravitinoConfig(null);
        OAuthData oauth = config.getOAuth();
        DefaultOAuth2TokenProvider tokenProvider =
            DefaultOAuth2TokenProvider.builder()
                .withUri(oauth.getServerURI())
                .withCredential(oauth.getCredential())
                .withPath(oauth.getToken())
                .withScope(oauth.getScope())
                .build();

        builder = builder.withOAuth(tokenProvider);
      } else if (authentication.equals(KERBEROS_AUTH)) {
        GravitinoConfig config = new GravitinoConfig(null);
        KerberosData kerberos = config.getKerberos();
        KerberosTokenProvider tokenProvider =
            KerberosTokenProvider.builder()
                .withClientPrincipal(kerberos.getPrincipal())
                .withKeyTabFile(new File(kerberos.getKeytabFile()))
                .build();

        builder = builder.withKerberosAuth(tokenProvider);
      } else {
        exitWithError("Unsupported authentication type " + authentication);
      }
    }

    return builder;
  }

  /**
   * Outputs the entity to the console.
   *
   * @param entity The entity to output.
   * @param <T> The type of entity.
   */
  protected <T> void output(T entity) {
    if (outputFormat == null) {
      PlainFormat.output(entity);
      return;
    }

    if (outputFormat.equals(OUTPUT_FORMAT_TABLE)) {
      TableFormat.output(entity);
    } else if (outputFormat.equals(OUTPUT_FORMAT_PLAIN)) {
      PlainFormat.output(entity);
    } else {
      throw new IllegalArgumentException("Unsupported output format");
    }
  }
}
