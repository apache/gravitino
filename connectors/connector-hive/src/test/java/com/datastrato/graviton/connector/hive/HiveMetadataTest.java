package com.datastrato.graviton.connector.hive;

import java.util.List;
import java.util.Optional;

import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.thrift.TException;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assume.assumeTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class HiveMetadataTest {
  static HiveMetadata hiveMetadata;

  private static final String TEST_DATABASE="HiveMetadataTestDB";
  private static final String TEST_DATABASE_DESCRIPTION="HiveMetadataTestDB description";
  private static final ImmutableMap<String, String> TEST_DATABASE_PARAMS =
          new ImmutableMap.Builder<String, String>()
                  .put("param", "value")
                  .build();
  @BeforeAll
  public static void beforeTest() {
    hiveMetadata = HiveMetadata.builder().build();
    Database db = new HiveUtils.DatabaseBuilder(TEST_DATABASE)
            .withDescription(TEST_DATABASE_DESCRIPTION)
            .withParams(TEST_DATABASE_PARAMS)
            .build();
    try {
      hiveMetadata.createDatabase(db);
    } catch (Exception e) {
      System.out.println(e.getMessage());
      e.printStackTrace();
    }
  }

  @AfterAll
  public static void afterTest() {
    try {
      hiveMetadata.dropDatabase(TEST_DATABASE, true);
    } catch (Exception e) {
      System.out.println(e.getMessage());
      e.printStackTrace();
    }
  }

  @Test
  public void getAllDatabases() throws TException {
    List<String> dbs = hiveMetadata.getAllDatabases("");
    assert dbs.size() == 2;
  }

  @Test
  public void createDatabaseNullName() {
    Database db = new HiveUtils.DatabaseBuilder(TEST_DATABASE).build();
    db.setName(null);
    Throwable exception = assertThrows(MetaException.class,
            () -> hiveMetadata.createDatabase(db));
  }

  @Test
  public void createExistingDatabase() {
    Database db = new HiveUtils.DatabaseBuilder(TEST_DATABASE).build();
    Throwable exception = assertThrows(AlreadyExistsException.class,
            () -> hiveMetadata.createDatabase(db));
  }

  @Test
  public void createDatabaseEmptyName() {
    Database db = new HiveUtils.DatabaseBuilder(TEST_DATABASE).build();
    db.setName("");
    Throwable exception = assertThrows(InvalidObjectException.class,
            () -> hiveMetadata.createDatabase(db));
  }

  @Test
  public void getDatabase() throws TException {
    Optional<Database> db = hiveMetadata.getDatabase(TEST_DATABASE);
    assert db.isPresent();

    assertEquals(db.get().getName(), TEST_DATABASE);
    assertEquals(db.get().getDescription(), TEST_DATABASE_DESCRIPTION);
    assertEquals(db.get().getParameters(), TEST_DATABASE_PARAMS);
    assertEquals(db.get().getLocationUri(), TEST_DATABASE.toLowerCase());
  }
}
