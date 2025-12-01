package org.apache.gravitino.cli.commands;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.Collections;
import org.apache.commons.cli.CommandLine;
import org.apache.gravitino.cli.CommandContext;
import org.apache.gravitino.cli.ErrorMessages;
import org.apache.gravitino.cli.Main;
import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.exceptions.NoSuchGroupException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestRemoveRoleFromGroup {

  private final ByteArrayOutputStream errContent = new ByteArrayOutputStream();
  private final PrintStream originalErr = System.err;

  @BeforeEach
  void setup() {
    System.setErr(new PrintStream(errContent));
  }

  @AfterEach
  void restore() {
    System.setErr(originalErr);
    Main.useExit = true;
  }

  @Test
  void handleWithMissingGroupExitsWithUnknownGroupError() throws Exception {
    Main.useExit = false;
    CommandLine mockCmdLine = mock(CommandLine.class);
    CommandContext context = new CommandContext(mockCmdLine);

    RemoveRoleFromGroup command =
        spy(new RemoveRoleFromGroup(context, "metalake1", "group1", "role1"));

    GravitinoClient mockClient = mock(GravitinoClient.class);
    doReturn(mockClient).when(command).buildClient("metalake1");

    doThrow(new NoSuchGroupException("group missing"))
        .when(mockClient)
        .revokeRolesFromGroup(Collections.singletonList("role1"), "group1");

    RuntimeException ex = assertThrows(RuntimeException.class, command::handle);

    assertTrue(ex.getMessage().contains("Exit with code -1"));
    assertTrue(errContent.toString(UTF_8).contains(ErrorMessages.UNKNOWN_GROUP));
  }
}
