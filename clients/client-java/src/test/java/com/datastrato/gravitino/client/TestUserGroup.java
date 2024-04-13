package com.datastrato.gravitino.client;

import com.datastrato.gravitino.authorization.Group;
import com.datastrato.gravitino.authorization.User;
import com.datastrato.gravitino.dto.AuditDTO;
import com.datastrato.gravitino.dto.authorization.GroupDTO;
import com.datastrato.gravitino.dto.authorization.UserDTO;
import com.datastrato.gravitino.dto.requests.GroupAddRequest;
import com.datastrato.gravitino.dto.requests.UserAddRequest;
import com.datastrato.gravitino.dto.responses.ErrorResponse;
import com.datastrato.gravitino.dto.responses.GroupResponse;
import com.datastrato.gravitino.dto.responses.RemoveResponse;
import com.datastrato.gravitino.dto.responses.UserResponse;
import java.time.Instant;
import org.apache.hc.core5.http.Method;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class TestUserGroup extends TestBase {

  private static final String API_METALAKES_USERS_PATH = "api/metalakes/%s/users/%s";
  private static final String API_METALAKES_GROUPS_PATH = "api/metalakes/%s/groups/%s";
  protected static final String metalakeName = "testMetalake";

  @BeforeAll
  public static void setUp() throws Exception {
    TestBase.setUp();
  }

  @Test
  public void testAddUsers() throws Exception {
    String username = "user";
    String userPath = withSlash(String.format(API_METALAKES_USERS_PATH, metalakeName, ""));
    UserAddRequest request = new UserAddRequest(username);

    UserDTO mockUser = mockUserDTO(username);
    UserResponse userResponse = new UserResponse(mockUser);
    buildMockResource(Method.POST, userPath, request, userResponse, SC_OK);

    User addedUser = client.addUser(metalakeName, username);
    Assertions.assertNotNull(addedUser);
    assertUser(addedUser, mockUser);

    // test UserAlreadyExistsException
    ErrorResponse errResp1 =
        ErrorResponse.alreadyExists(
            UserAlreadyExistsException.class.getSimpleName(), "user already exists");
    buildMockResource(Method.POST, userPath, request, errResp1, SC_CONFLICT);
    Exception ex =
        Assertions.assertThrows(
            UserAlreadyExistsException.class, () -> client.addUser(metalakeName, username));
    Assertions.assertEquals("user already exists", ex.getMessage());

    // test NoSuchMetalakeException
    ErrorResponse errResp2 =
        ErrorResponse.notFound(NoSuchMetalakeException.class.getSimpleName(), "metalake not found");
    buildMockResource(Method.POST, userPath, request, errResp2, SC_NOT_FOUND);
    ex =
        Assertions.assertThrows(
            NoSuchMetalakeException.class, () -> client.addUser(metalakeName, username));
    Assertions.assertEquals("metalake not found", ex.getMessage());

    // test RuntimeException
    ErrorResponse errResp3 = ErrorResponse.internalError("internal error");
    buildMockResource(Method.POST, userPath, request, errResp3, SC_SERVER_ERROR);
    Assertions.assertThrows(
        RuntimeException.class, () -> client.getUser(metalakeName, username), "internal error");
  }

  @Test
  public void testGetUsers() throws Exception {
    String username = "user";
    String userPath = withSlash(String.format(API_METALAKES_USERS_PATH, metalakeName, username));

    UserDTO mockUser = mockUserDTO(username);
    UserResponse userResponse = new UserResponse(mockUser);
    buildMockResource(Method.GET, userPath, null, userResponse, SC_OK);

    User loadedUser = client.getUser(metalakeName, username);
    Assertions.assertNotNull(loadedUser);
    assertUser(mockUser, loadedUser);

    // test NoSuchUserException
    ErrorResponse errResp1 =
        ErrorResponse.notFound(NoSuchUserException.class.getSimpleName(), "user not found");
    buildMockResource(Method.GET, userPath, null, errResp1, SC_NOT_FOUND);
    Exception ex =
        Assertions.assertThrows(
            NoSuchUserException.class, () -> client.getUser(metalakeName, username));
    Assertions.assertEquals("user not found", ex.getMessage());

    // test NoSuchMetalakeException
    ErrorResponse errResp2 =
        ErrorResponse.notFound(NoSuchMetalakeException.class.getSimpleName(), "metalake not found");
    buildMockResource(Method.GET, userPath, null, errResp2, SC_NOT_FOUND);
    ex =
        Assertions.assertThrows(
            NoSuchMetalakeException.class, () -> client.getUser(metalakeName, username));
    Assertions.assertEquals("metalake not found", ex.getMessage());

    // test RuntimeException
    ErrorResponse errResp3 = ErrorResponse.internalError("internal error");
    buildMockResource(Method.GET, userPath, null, errResp3, SC_SERVER_ERROR);
    Assertions.assertThrows(
        RuntimeException.class, () -> client.getUser(metalakeName, username), "internal error");
  }

  @Test
  public void testRemoveUsers() throws Exception {
    String username = "user";
    String userPath = withSlash(String.format(API_METALAKES_USERS_PATH, metalakeName, username));

    RemoveResponse removeResponse = new RemoveResponse(true);
    buildMockResource(Method.DELETE, userPath, null, removeResponse, SC_OK);

    Assertions.assertTrue(client.removeUser(metalakeName, username));

    removeResponse = new RemoveResponse(false);
    buildMockResource(Method.DELETE, userPath, null, removeResponse, SC_OK);
    Assertions.assertFalse(client.removeUser(metalakeName, username));

    // test RuntimeException
    ErrorResponse errResp = ErrorResponse.internalError("internal error");
    buildMockResource(Method.DELETE, userPath, null, errResp, SC_SERVER_ERROR);
    Assertions.assertFalse(client.removeUser(metalakeName, username));
  }

  @Test
  public void testAddGroups() throws Exception {
    String groupName = "group";
    String groupPath = withSlash(String.format(API_METALAKES_GROUPS_PATH, metalakeName, ""));
    GroupAddRequest request = new GroupAddRequest(groupName);

    GroupDTO mockGroup = mockGroupDTO(groupName);
    GroupResponse groupResponse = new GroupResponse(mockGroup);
    buildMockResource(Method.POST, groupPath, request, groupResponse, SC_OK);

    Group addedGroup = client.addGroup(metalakeName, groupName);
    Assertions.assertNotNull(addedGroup);
    assertGroup(addedGroup, mockGroup);

    // test GroupAlreadyExistsException
    ErrorResponse errResp1 =
        ErrorResponse.alreadyExists(
            GroupAlreadyExistsException.class.getSimpleName(), "group already exists");
    buildMockResource(Method.POST, groupPath, request, errResp1, SC_CONFLICT);
    Exception ex =
        Assertions.assertThrows(
            GroupAlreadyExistsException.class, () -> client.addGroup(metalakeName, groupName));
    Assertions.assertEquals("group already exists", ex.getMessage());

    // test NoSuchMetalakeException
    ErrorResponse errResp2 =
        ErrorResponse.notFound(NoSuchMetalakeException.class.getSimpleName(), "metalake not found");
    buildMockResource(Method.POST, groupPath, request, errResp2, SC_NOT_FOUND);
    ex =
        Assertions.assertThrows(
            NoSuchMetalakeException.class, () -> client.addGroup(metalakeName, groupName));
    Assertions.assertEquals("metalake not found", ex.getMessage());

    // test RuntimeException
    ErrorResponse errResp3 = ErrorResponse.internalError("internal error");
    buildMockResource(Method.POST, groupPath, request, errResp3, SC_SERVER_ERROR);
    Assertions.assertThrows(
        RuntimeException.class, () -> client.addGroup(metalakeName, groupName), "internal error");
  }

  @Test
  public void testGetGroups() throws Exception {
    String groupName = "group";
    String groupPath = withSlash(String.format(API_METALAKES_GROUPS_PATH, metalakeName, groupName));

    GroupDTO mockGroup = mockGroupDTO(groupName);
    GroupResponse groupResponse = new GroupResponse(mockGroup);
    buildMockResource(Method.GET, groupPath, null, groupResponse, SC_OK);

    Group loadedGroup = client.getGroup(metalakeName, groupName);
    Assertions.assertNotNull(loadedGroup);
    assertGroup(mockGroup, loadedGroup);

    // test NoSuchGroupException
    ErrorResponse errResp1 =
        ErrorResponse.notFound(NoSuchGroupException.class.getSimpleName(), "group not found");
    buildMockResource(Method.GET, groupPath, null, errResp1, SC_NOT_FOUND);
    Exception ex =
        Assertions.assertThrows(
            NoSuchGroupException.class, () -> client.getGroup(metalakeName, groupName));
    Assertions.assertEquals("group not found", ex.getMessage());

    // test NoSuchMetalakeException
    ErrorResponse errResp2 =
        ErrorResponse.notFound(NoSuchMetalakeException.class.getSimpleName(), "metalake not found");
    buildMockResource(Method.GET, groupPath, null, errResp2, SC_NOT_FOUND);
    ex =
        Assertions.assertThrows(
            NoSuchMetalakeException.class, () -> client.getGroup(metalakeName, groupName));
    Assertions.assertEquals("metalake not found", ex.getMessage());

    // test RuntimeException
    ErrorResponse errResp3 = ErrorResponse.internalError("internal error");
    buildMockResource(Method.GET, groupPath, null, errResp3, SC_SERVER_ERROR);
    Assertions.assertThrows(
        RuntimeException.class, () -> client.getGroup(metalakeName, groupName), "internal error");
  }

  @Test
  public void testRemoveGroups() throws Exception {
    String groupName = "user";
    String groupPath = withSlash(String.format(API_METALAKES_GROUPS_PATH, metalakeName, groupName));

    RemoveResponse removeResponse = new RemoveResponse(true);
    buildMockResource(Method.DELETE, groupPath, null, removeResponse, SC_OK);

    Assertions.assertTrue(client.removeGroup(metalakeName, groupName));

    removeResponse = new RemoveResponse(false);
    buildMockResource(Method.DELETE, groupPath, null, removeResponse, SC_OK);
    Assertions.assertFalse(client.removeGroup(metalakeName, groupName));

    // test RuntimeException
    ErrorResponse errResp = ErrorResponse.internalError("internal error");
    buildMockResource(Method.DELETE, groupPath, null, errResp, SC_SERVER_ERROR);
    Assertions.assertFalse(client.removeGroup(metalakeName, groupName));
  }

  private UserDTO mockUserDTO(String name) {
    return UserDTO.builder()
        .withName(name)
        .withAudit(AuditDTO.builder().withCreator("creator").withCreateTime(Instant.now()).build())
        .build();
  }

  private GroupDTO mockGroupDTO(String name) {
    return GroupDTO.builder()
        .withName(name)
        .withAudit(AuditDTO.builder().withCreator("creator").withCreateTime(Instant.now()).build())
        .build();
  }

  private void assertUser(User expected, User actual) {
    Assertions.assertEquals(expected.name(), actual.name());
    Assertions.assertEquals(expected.roles(), actual.roles());
  }

  private void assertGroup(Group expected, Group actual) {
    Assertions.assertEquals(expected.name(), actual.name());
    Assertions.assertEquals(expected.roles(), actual.roles());
  }
}
