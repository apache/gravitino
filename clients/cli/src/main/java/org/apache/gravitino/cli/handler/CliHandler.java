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

package org.apache.gravitino.cli.handler;

import com.google.common.base.Joiner;
import java.io.File;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.function.Supplier;
import org.apache.gravitino.cli.CliFullName;
import org.apache.gravitino.cli.ErrorMessages;
import org.apache.gravitino.cli.GravitinoConfig;
import org.apache.gravitino.cli.GravitinoOptions;
import org.apache.gravitino.cli.KerberosData;
import org.apache.gravitino.cli.MainCli;
import org.apache.gravitino.cli.OAuthData;
import org.apache.gravitino.cli.options.CommonOptions;
import org.apache.gravitino.cli.outputs.BaseOutputFormat;
import org.apache.gravitino.cli.outputs.OutputFormat;
import org.apache.gravitino.cli.outputs.PlainFormat;
import org.apache.gravitino.cli.outputs.TableFormat;
import org.apache.gravitino.client.DefaultOAuth2TokenProvider;
import org.apache.gravitino.client.GravitinoAdminClient;
import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.client.GravitinoClientBase;
import org.apache.gravitino.client.KerberosTokenProvider;
import org.apache.gravitino.exceptions.CatalogInUseException;
import org.apache.gravitino.exceptions.MetalakeInUseException;
import org.apache.gravitino.exceptions.NoSuchCatalogException;
import org.apache.gravitino.exceptions.NoSuchColumnException;
import org.apache.gravitino.exceptions.NoSuchFilesetException;
import org.apache.gravitino.exceptions.NoSuchGroupException;
import org.apache.gravitino.exceptions.NoSuchMetalakeException;
import org.apache.gravitino.exceptions.NoSuchModelException;
import org.apache.gravitino.exceptions.NoSuchModelVersionException;
import org.apache.gravitino.exceptions.NoSuchRoleException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.exceptions.NoSuchTableException;
import org.apache.gravitino.exceptions.NoSuchTopicException;
import org.apache.gravitino.exceptions.NoSuchUserException;
import picocli.CommandLine;

/** Base class for all cli handlers. will be renamed as Command when */
public abstract class CliHandler implements Callable<Integer> {

  /** The style of the head heading */
  public static final String HEAD_HEADING_STYLE = "@|bold,underline Usage|@:%n%n";
  /** The style of the synopsis heading */
  public static final String SYNOPSIS_HEADING_STYLE = "%n";
  /** The style of the description heading */
  public static final String DESCRIPTION_HEADING_STYLE = "%n@|bold,underline Description|@:%n%n";
  /** The style of the parameter list heading */
  public static final String PARAMETER_LIST_HEADING_STYLE = "%n@|bold,underline Options|@:%n%n";
  /** The style of the option list heading */
  public static final String OPTION_LIST_HEADING_STYLE = "%n@|bold,underline Options|@:%n";

  /** The joiner for comma-separated values. */
  public static final Joiner COMMA_JOINER = Joiner.on(", ").skipNulls();

  private static final String SIMPLE_AUTH = "simple";

  private static final String OAUTH_AUTH = "oauth";
  private static final String KERBEROS_AUTH = "kerberos";

  /** The common options for the command. */
  @CommandLine.Mixin protected CommonOptions commonOptions;

  /** The command spec, injected by picocli. */
  @CommandLine.Spec protected CommandLine.Model.CommandSpec spec;

  private CliFullName fullName;
  private String authEnv;
  private boolean authSet = false;
  private String metalake;
  private boolean ignoreVersions;
  private OutputFormat.OutputType outputFormat;
  private String authentication;
  private String url;
  private String userName;
  private boolean quiet;

  /**
   * Validate the options for the command, check whether the required options are present. or wheter
   * the options are conflict.
   *
   * @param validator The name validator for the command.
   */
  protected void validateOptions(NameValidator validator) {
    this.fullName = new CliFullName(spec);
    this.metalake = fullName.getMetalakeName();
    this.authentication = getAuth();
    this.ignoreVersions = commonOptions.ignoreVersions;
    this.outputFormat = commonOptions.outputFormat;
    this.url = commonOptions.url;
    this.userName = commonOptions.login;
    this.quiet = commonOptions.quiet;

    if (metalake == null) {
      MainCli.exit(-1);
    }

    List<String> missingEntities = validator.getMissingEntities(fullName);
    if (!missingEntities.isEmpty()) {
      throw new RuntimeException(
          ErrorMessages.MISSING_ENTITIES + COMMA_JOINER.join(missingEntities));
    }
  }

  /** {@inheritDoc} */
  @Override
  public Integer call() throws Exception {
    NameValidator validator = createValidator();
    validateOptions(validator);

    return doCall();
  }

  /**
   * Prints an error message and exits with a non-zero status.
   *
   * @param error The error message to display before exiting.
   */
  protected void exitWithError(String error) {
    System.err.println(error);
    MainCli.exit(-1);
  }

  /**
   * Execute the command
   *
   * @param function The execute command
   * @return The result of command
   * @param <T> The type of result.
   */
  protected <T> T execute(Supplier<T> function) {
    try {
      return function.get();
    } catch (NoSuchMetalakeException err) {
      exitWithError(ErrorMessages.UNKNOWN_METALAKE);
    } catch (NoSuchCatalogException err) {
      exitWithError(ErrorMessages.UNKNOWN_CATALOG);
    } catch (NoSuchSchemaException err) {
      exitWithError(ErrorMessages.UNKNOWN_SCHEMA);
    } catch (NoSuchTableException err) {
      exitWithError(ErrorMessages.UNKNOWN_TABLE);
    } catch (NoSuchFilesetException err) {
      exitWithError(ErrorMessages.UNKNOWN_FILESET);
    } catch (NoSuchTopicException err) {
      exitWithError(ErrorMessages.UNKNOWN_TOPIC);
    } catch (NoSuchColumnException err) {
      exitWithError(ErrorMessages.UNKNOWN_COLUMN);
    } catch (NoSuchModelException err) {
      exitWithError(ErrorMessages.UNKNOWN_MODEL);
    } catch (NoSuchModelVersionException err) {
      exitWithError(ErrorMessages.UNKNOWN_MODEL_VERSION);
    } catch (NoSuchRoleException err) {
      exitWithError(ErrorMessages.UNKNOWN_ROLE);
    } catch (NoSuchUserException err) {
      exitWithError(ErrorMessages.UNKNOWN_USER);
    } catch (NoSuchGroupException err) {
      exitWithError(ErrorMessages.UNKNOWN_GROUP);
    } catch (MetalakeInUseException err) {
      exitWithError("Metalake" + ErrorMessages.ENTITY_IN_USE);
    } catch (CatalogInUseException catalogInUseException) {
      exitWithError("Catalog" + ErrorMessages.ENTITY_IN_USE);
    } catch (Exception exp) {
      exitWithError(exp.getMessage());
    }

    return null;
  }

  /**
   * Prints out the results of the command.
   *
   * @param results The results to print.
   */
  public void printResults(String results) {
    BaseOutputFormat.output(results, System.out);
  }

  /**
   * Prints out the results of the command.
   *
   * @param entity The entity to print.
   * @param <T> The type of entity.
   */
  public <T> void printResults(T entity) {
    output(entity);
  }

  /**
   * Get the name of the metalake for the command.
   *
   * @return The name of the metalake for the command.
   */
  protected String getMetalake() {
    return metalake;
  }

  /**
   * Get the name of the schema for the command.
   *
   * @return The name of the schema for the command.
   */
  protected String getCatalog() {
    return fullName.getCatalogName();
  }

  /**
   * Get the name of the schema for the command.
   *
   * @return The name of the schema for the command.
   */
  protected String getSchema() {
    return fullName.getSchemaName();
  }

  /**
   * Get the url
   *
   * @return The url
   */
  protected String getUrl() {
    return url;
  }

  /**
   * Prints out an informational message, often to indicate a command has finished.
   *
   * @param message The message to display.
   */
  protected void printInformation(String message) {
    if (quiet) {
      return;
    }

    printResults(message);
  }

  /**
   * Builds a {@link GravitinoClient} instance with the provided server URL and metalake.
   *
   * @return A configured {@link GravitinoClient} instance.
   * @throws NoSuchMetalakeException if the specified metalake does not exist.
   */
  protected GravitinoClient buildClient() {
    try {
      return buildClient(metalake);
    } catch (NoSuchMetalakeException err) {
      exitWithError(ErrorMessages.UNKNOWN_METALAKE);
    }

    return null;
  }

  /**
   * Builds a {@link GravitinoAdminClient} instance with the server URL.
   *
   * @return A configured {@link GravitinoAdminClient} instance.
   */
  protected GravitinoAdminClient buildAdminClient() {
    GravitinoClientBase.Builder<GravitinoAdminClient> client = GravitinoAdminClient.builder(url);

    return constructClient(client).build();
  }

  /**
   * Configures and constructs a {@link GravitinoClientBase.Builder} instance for creating a {@link
   * GravitinoClient} or {@link GravitinoAdminClient}.
   *
   * @param builder The {@link GravitinoClientBase.Builder} instance to be configured.
   * @param <T> The type of the {@link GravitinoClientBase}.
   * @return A configured {@link GravitinoClientBase.Builder} instance.
   */
  protected <T extends GravitinoClientBase> GravitinoClientBase.Builder<T> constructClient(
      GravitinoClientBase.Builder<T> builder) {
    if (ignoreVersions) {
      builder = builder.withVersionCheckDisabled();
    }

    if (authentication == null) {
      return builder;
    }

    if (SIMPLE_AUTH.equals(authentication)) {
      if (userName != null && !userName.isEmpty()) {
        builder = builder.withSimpleAuth(userName);
      } else {
        builder = builder.withSimpleAuth();
      }
    } else if (OAUTH_AUTH.equals(authentication)) {
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
    } else if (KERBEROS_AUTH.equals(authentication)) {
      GravitinoConfig config = new GravitinoConfig(null);
      KerberosData kerberos = config.getKerberos();

      KerberosTokenProvider tokenProvider =
          KerberosTokenProvider.builder()
              .withClientPrincipal(kerberos.getPrincipal())
              .withKeyTabFile(new File(kerberos.getKeytabFile()))
              .build();

      builder = builder.withKerberosAuth(tokenProvider);
    } else {
      System.err.println("Invalid authentication type: " + authentication);
    }

    return builder;
  }

  private GravitinoClient buildClient(String metalake) throws NoSuchMetalakeException {
    GravitinoClientBase.Builder<GravitinoClient> client =
        GravitinoClient.builder(url).withMetalake(metalake);

    return constructClient(client).build();
  }

  private String getAuth() {
    // If specified on the command line use that
    if (commonOptions.simple) {
      return GravitinoOptions.SIMPLE;
    }

    // Cache the Gravitino authentication type environment variable
    if (authEnv == null && !authSet) {
      authEnv = System.getenv("GRAVITINO_AUTH");
      authSet = true;
    }

    // If set return the Gravitino authentication type environment variable
    if (authEnv != null) {
      return authEnv;
    }

    // Check if the authentication type is specified in the configuration file
    GravitinoConfig config = new GravitinoConfig(null);
    if (config.fileExists()) {
      config.read();
      String configAuthType = config.getGravitinoAuthType();
      if (configAuthType != null) {
        return configAuthType;
      }
    }

    return null;
  }

  private <T> void output(T entity) {
    if (outputFormat.equals(OutputFormat.OutputType.TABLE)) {
      TableFormat.output(entity, null);
    } else if (outputFormat.equals(OutputFormat.OutputType.PLAIN)) {
      PlainFormat.output(entity, null);
    } else {
      throw new IllegalArgumentException("Unsupported output format");
    }
  }

  /**
   * Execute the command.
   *
   * @return The exit code of the command.
   * @throws Exception If an error occurs during execution.C
   */
  protected abstract Integer doCall() throws Exception;

  /**
   * Initialize the name option validator for the command.
   *
   * @return The name validator for the command.
   */
  protected abstract NameValidator createValidator();
}
