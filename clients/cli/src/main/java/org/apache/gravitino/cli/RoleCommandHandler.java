package org.apache.gravitino.cli;

import com.google.common.base.Preconditions;
import java.util.Arrays;
import org.apache.commons.cli.CommandLine;
import org.apache.gravitino.cli.commands.Command;

public class RoleCommandHandler extends CommandHandler {

  private final GravitinoCommandLine gravitinoCommandLine;
  private final CommandLine line;
  private final String command;
  private final boolean ignore;
  private final String url;
  private String metalake;

  public RoleCommandHandler(
      GravitinoCommandLine gravitinoCommandLine, CommandLine line, String command, boolean ignore) {
    this.gravitinoCommandLine = gravitinoCommandLine;
    this.line = line;
    this.command = command;
    this.ignore = ignore;
    this.url = getUrl(line);
  }

  /** Handles the command execution logic based on the provided command. */
  public void handle() {
    String auth = getAuth(line);
    String userName = line.getOptionValue(GravitinoOptions.LOGIN);
    Command.setAuthenticationMode(auth, userName);

    metalake = new FullName(line).getMetalakeName();

    String[] roles = line.getOptionValues(GravitinoOptions.ROLE);
    if (roles == null && !CommandActions.LIST.equals(command)) {
      System.err.println(ErrorMessages.MISSING_ROLE);
      Main.exit(-1);
    }
    if (roles != null) {
      roles = Arrays.stream(roles).distinct().toArray(String[]::new);
    }

    String[] privileges = line.getOptionValues(GravitinoOptions.PRIVILEGE);

    if (!executeCommand(roles, privileges)) {
      System.err.println(ErrorMessages.UNSUPPORTED_ACTION);
      Main.exit(-1);
    }
  }

  /**
   * Executes the specific command based on the command type.
   *
   * @return true if the command is supported, false otherwise
   */
  private boolean executeCommand(String[] roles, String[] privileges) {
    switch (command) {
      case CommandActions.DETAILS:
        handleDetailsCommand(roles);
        return true;

      case CommandActions.LIST:
        handleListCommand();
        return true;

      case CommandActions.CREATE:
        handleCreateCommand(roles);
        return true;

      case CommandActions.DELETE:
        handleDeleteCommand(roles);
        return true;

      case CommandActions.GRANT:
        handleGrantCommand(roles, privileges);
        return true;

      case CommandActions.REVOKE:
        handleRevokeCommand(roles, privileges);
        return true;

      default:
        return false;
    }
  }

  private void handleDetailsCommand(String[] roles) {
    if (line.hasOption(GravitinoOptions.AUDIT)) {
      gravitinoCommandLine
          .newRoleAudit(url, ignore, metalake, getOneRole(roles, CommandActions.DETAILS))
          .handle();
    } else {
      gravitinoCommandLine
          .newRoleDetails(url, ignore, metalake, getOneRole(roles, CommandActions.DETAILS))
          .handle();
    }
  }

  private void handleListCommand() {
    gravitinoCommandLine.newListRoles(url, ignore, metalake).handle();
  }

  private void handleCreateCommand(String[] roles) {
    gravitinoCommandLine.newCreateRole(url, ignore, metalake, roles).handle();
  }

  private void handleDeleteCommand(String[] roles) {
    boolean forceDelete = line.hasOption(GravitinoOptions.FORCE);
    gravitinoCommandLine.newDeleteRole(url, ignore, forceDelete, metalake, roles).handle();
  }

  private void handleGrantCommand(String[] roles, String[] privileges) {
    gravitinoCommandLine
        .newGrantPrivilegesToRole(
            url,
            ignore,
            metalake,
            getOneRole(roles, CommandActions.GRANT),
            new FullName(line),
            privileges)
        .handle();
  }

  private void handleRevokeCommand(String[] roles, String[] privileges) {
    gravitinoCommandLine
        .newRevokePrivilegesFromRole(
            url,
            ignore,
            metalake,
            getOneRole(roles, CommandActions.REMOVE),
            new FullName(line),
            privileges)
        .handle();
  }

  private String getOneRole(String[] roles, String command) {
    Preconditions.checkArgument(
        roles != null && roles.length == 1,
        command + " requires exactly one role, but multiple are currently passed.");
    return roles[0];
  }
}
