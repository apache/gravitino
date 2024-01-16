/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.integration.test.trino;

import static io.trino.cli.ClientOptions.OutputFormat.CSV;

import io.airlift.units.Duration;
import io.trino.cli.Query;
import io.trino.cli.QueryRunner;
import io.trino.cli.TerminalUtils;
import io.trino.client.ClientSession;
import io.trino.client.uri.TrinoUri;
import java.io.PrintStream;
import java.net.URI;
import java.time.ZoneId;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import jodd.io.StringOutputStream;
import okhttp3.logging.HttpLoggingInterceptor;
import org.jline.terminal.Terminal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class TrinoQueryRunner {
  private static final Logger LOG = LoggerFactory.getLogger(TrinoQueryRunner.class);

  private QueryRunner queryRunner;
  private Terminal terminal;
  private URI uri;

  TrinoQueryRunner(String trinoUri) throws Exception {
    this.uri = new URI(trinoUri);
    this.queryRunner = createQueryRunner();
    this.terminal = TerminalUtils.getTerminal();
  }

  private QueryRunner createQueryRunner() throws Exception {

    TrinoUri trinoUri = TrinoUri.builder().setUri(uri).build();

    ClientSession session =
        ClientSession.builder()
            .server(uri)
            .user(Optional.of("admin"))
            .timeZone(ZoneId.systemDefault())
            .clientRequestTimeout(new Duration(30, TimeUnit.SECONDS))
            .build();
    return new QueryRunner(trinoUri, session, false, HttpLoggingInterceptor.Level.NONE);
  }

  String runQuery(String query) {
    Query queryResult = queryRunner.startQuery(query);
    StringOutputStream outputStream = new StringOutputStream();
    queryResult.renderOutput(
        this.terminal,
        new PrintStream(outputStream),
        new PrintStream(outputStream),
        CSV,
        Optional.of(""),
        false);
    ClientSession session = queryRunner.getSession();

    // update catalog and schema if present
    if (queryResult.getSetCatalog().isPresent() || queryResult.getSetSchema().isPresent()) {
      ClientSession.Builder builder = ClientSession.builder(session);
      queryResult.getSetCatalog().ifPresent(builder::catalog);
      queryResult.getSetSchema().ifPresent(builder::schema);
      session = builder.build();
      queryRunner.setSession(session);
    }
    return outputStream.toString();
  }

  boolean stop() {
    try {
      queryRunner.close();
      terminal.close();
      return true;
    } catch (Exception e) {
      LOG.error("Failed to stop query runner", e);
      return false;
    }
  }
}
