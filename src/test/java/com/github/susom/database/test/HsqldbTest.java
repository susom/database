/*
 * Copyright 2016 The Board of Trustees of The Leland Stanford Junior University.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.susom.database.test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Ignore;
import org.junit.Test;

import com.github.susom.database.Config;
import com.github.susom.database.ConfigFrom;
import com.github.susom.database.DatabaseException;
import com.github.susom.database.DatabaseProvider;
import com.github.susom.database.OptionsOverride;
import com.github.susom.database.Schema;
import com.github.susom.database.Sql;
import com.github.susom.database.SqlArgs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Exercise database functionality with a real HyperSQL database.
 *
 * @author garricko
 */
public class HsqldbTest extends CommonTest {
  @Override
  protected DatabaseProvider createDatabaseProvider(OptionsOverride options) throws Exception {
    String propertiesFile = System.getProperty("local.properties", "local.properties");
    Config config = ConfigFrom.firstOf()
        .systemProperties()
        .propertyFile(propertiesFile)
        .excludePrefix("database.")
        .removePrefix("hsqldb.").get();
    return DatabaseProvider.fromDriverManager(config)
        .withSqlParameterLogging()
        .withSqlInExceptionMessages()
        .withOptions(options).create();
  }

  @Test
  public void noDatabaseAccess() throws Exception {
    DatabaseProvider provider = createDatabaseProvider(new OptionsOverride());
    provider.transact(dbp -> {
      // Do nothing, just making sure no exception is thrown
    });
    provider.transact((dbp, tx) -> {
      // Do nothing, just making sure no exception is thrown
    });
    provider.transact((dbp, tx) -> {
      tx.setRollbackOnError(true);
      // Do nothing, just making sure no exception is thrown
    });
    provider.transact((dbp, tx) -> {
      tx.setRollbackOnly(true);
      // Do nothing, just making sure no exception is thrown
    });
  }

  @Ignore("LocalDate implementations should be TimeZone agnostic, but HSQLDB implementation has a bug.")
  @Test
  public void argLocalDateTimeZones() {
    // See bug: https://bugs.documentfoundation.org/show_bug.cgi?id=63566
    super.argLocalDateTimeZones();
  }

  /**
   * This one is adjusted in that the float values are passed as double, because
   * the database stores them both as double and there doesn't appear to be a way
   * to tell that one was actually declared as a float.
   */
  @Test
  public void saveResultAsTable() {
    new Schema().addTable("dbtest")
        .addColumn("nbr_integer").asInteger().primaryKey().table()
        .addColumn("nbr_long").asLong().table()
        .addColumn("nbr_float").asFloat().table()
        .addColumn("nbr_double").asDouble().table()
        .addColumn("nbr_big_decimal").asBigDecimal(19, 9).table()
        .addColumn("str_varchar").asString(80).table()
        .addColumn("str_fixed").asStringFixed(1).table()
        .addColumn("str_lob").asClob().table()
        .addColumn("bin_blob").asBlob().table()
        .addColumn("boolean_flag").asBoolean().table()
        .addColumn("date_millis").asDate().table()
        .addColumn("local_date").asLocalDate().schema().execute(db);

    db.toInsert("insert into dbtest (nbr_integer, nbr_long, nbr_float, nbr_double, nbr_big_decimal, str_varchar,"
        + " str_fixed, str_lob, bin_blob, boolean_flag, date_millis, local_date) values (?,?,?,?,?,?,?,?,?,?,?,?)")
        .argInteger(Integer.MAX_VALUE).argLong(Long.MAX_VALUE).argDouble((double) Float.MAX_VALUE)
        .argDouble(Double.MAX_VALUE).argBigDecimal(new BigDecimal("123.456"))
        .argString("hello").argString("Z").argClobString("hello again")
        .argBlobBytes(new byte[] { '1', '2' }).argBoolean(true)
      .argDateNowPerApp().argLocalDate(localDateNow).insert(1);

    db.toInsert("insert into dbtest (nbr_integer, nbr_long, nbr_float, nbr_double, nbr_big_decimal, str_varchar,"
        + " str_fixed, str_lob, bin_blob, boolean_flag, date_millis, local_date) values (?,?,?,?,?,?,?,?,?,?,?,?)")
        .argInteger(Integer.MIN_VALUE).argLong(Long.MIN_VALUE).argDouble(0.000001d)
        .argDouble(Double.MIN_VALUE).argBigDecimal(new BigDecimal("-123.456"))
        .argString("goodbye").argString("A").argClobString("bye again")
        .argBlobBytes(new byte[] { '3', '4' }).argBoolean(false)
        .argDateNowPerApp().argLocalDate(localDateNow).insert(1);

    String expectedSchema = new Schema().addTable("dbtest2")
        .addColumn("nbr_integer").asInteger().table()
        .addColumn("nbr_long").asLong().table()
        .addColumn("nbr_float").asFloat().table()
        .addColumn("nbr_double").asDouble().table()
        .addColumn("nbr_big_decimal").asBigDecimal(19, 9).table()
        .addColumn("str_varchar").asString(80).table()
        .addColumn("str_fixed").asStringFixed(1).table()
        .addColumn("str_lob").asClob().table()
        .addColumn("bin_blob").asBlob().table()
        .addColumn("boolean_flag").asBoolean().table()
        .addColumn("date_millis").asDate().table()
        .addColumn("local_date").asLocalDate().schema().print(db.flavor());

    List<SqlArgs> args = db.toSelect("select nbr_integer, nbr_long, nbr_float, nbr_double, nbr_big_decimal,"
        + " str_varchar, str_fixed, str_lob, bin_blob, boolean_flag, date_millis, local_date from dbtest")
        .query(rs -> {
          List<SqlArgs> result = new ArrayList<>();
          while (rs.next()) {
            if (result.size() == 0) {
              db.dropTableQuietly("dbtest2");
              Schema schema = new Schema().addTableFromRow("dbtest2", rs).schema();
              assertEquals(expectedSchema, schema.print(db.flavor()));
              schema.execute(db);
            }
            result.add(SqlArgs.readRow(rs));
          }
          return result;
        });

    db.toInsert(Sql.insert("dbtest2", args)).insertBatch();

    assertEquals(2, db.toSelect("select count(*) from dbtest2").queryIntegerOrZero());
    assertEquals(db.toSelect("select nbr_integer, nbr_long, nbr_float, nbr_double, nbr_big_decimal,"
            + " str_varchar, str_fixed, str_lob, bin_blob, boolean_flag, date_millis, local_date from dbtest order by 1")
            .queryMany(SqlArgs::readRow),
        db.toSelect("select nbr_integer, nbr_long, nbr_float, nbr_double, nbr_big_decimal,"
            + " str_varchar, str_fixed, str_lob, bin_blob, boolean_flag, date_millis, local_date from dbtest2 order by 1")
            .queryMany(SqlArgs::readRow));
    assertEquals(Arrays.asList(
      new SqlArgs()
            .argInteger("nbr_integer", Integer.MIN_VALUE)
            .argLong("nbr_long", Long.MIN_VALUE)
            .argDouble("nbr_float", 0.000001d)
            .argDouble("nbr_double", Double.MIN_VALUE)
            .argBigDecimal("nbr_big_decimal", new BigDecimal("-123.456"))
            .argString("str_varchar", "goodbye")
            .argString("str_fixed", "A")
            .argClobString("str_lob", "bye again")
            .argBlobBytes("bin_blob", new byte[] { '3', '4' })
            .argString("boolean_flag", "N")//.argBoolean("boolean_flag", false)
            .argDate("date_millis", now)
            .argLocalDate("local_date", localDateNow),
      new SqlArgs()
            .argInteger("nbr_integer", Integer.MAX_VALUE)
            .argLong("nbr_long", Long.MAX_VALUE)
            .argDouble("nbr_float", (double) Float.MAX_VALUE)
            .argDouble("nbr_double", Double.MAX_VALUE)
            .argBigDecimal("nbr_big_decimal", new BigDecimal("123.456"))
            .argString("str_varchar", "hello")
            .argString("str_fixed", "Z")
            .argClobString("str_lob", "hello again")
            .argBlobBytes("bin_blob", new byte[] { '1', '2' })
            .argString("boolean_flag", "Y")//.argBoolean("boolean_flag", true)
            .argDate("date_millis", now)
            .argLocalDate("local_date", localDateNow)),
      db.toSelect("select nbr_integer, nbr_long, nbr_float, nbr_double, nbr_big_decimal,"
        + " str_varchar, str_fixed, str_lob, bin_blob, boolean_flag, date_millis, local_date from dbtest2 order by 1")
        .queryMany(SqlArgs::readRow));
  }

  @Test
  public void identityColumn() {
    // Clean up table in case it exists from previous test
    db.dropTableQuietly("identity_test");
    
    // Test schema creation with identity column
    new Schema()
        .addTable("identity_test")
        .addColumn("id").primaryKeyIdentity().table()
        .addColumn("name").asString(50).table()
        .schema()
        .execute(db);

    // Test insert with identity column - should not use any argPk* methods
    Long generatedId1 = db.toInsert("insert into identity_test (name) values (?)")
        .argString("Test Record 1")
        .insertReturningPkDefault("id");

    // Test another insert to verify incrementing
    Long generatedId2 = db.toInsert("insert into identity_test (name) values (?)")
        .argString("Test Record 2")
        .insertReturningPkDefault("id");

    // Verify the records were inserted with proper generated IDs
    String name1 = db.toSelect("select name from identity_test where id = ?")
        .argLong(generatedId1)
        .queryStringOrNull();
    String name2 = db.toSelect("select name from identity_test where id = ?")
        .argLong(generatedId2)
        .queryStringOrNull();

    assertEquals("Test Record 1", name1);
    assertEquals("Test Record 2", name2);
    
    // Verify IDs are different (one should be generated after the other)
    assertNotEquals(generatedId1, generatedId2);
    
    // Verify we can't use argPk* methods with insertReturningPkDefault
    try {
      db.toInsert("insert into identity_test (id, name) values (?, ?)")
          .argPkLong(999L)
          .argString("Should fail")
          .insertReturningPkDefault("id");
      fail("Should have thrown DatabaseException");
    } catch (DatabaseException e) {
      assertTrue(e.getMessage().contains("Do not call argPk*() methods when using insertReturningPkDefault()"));
    }
  }


}
