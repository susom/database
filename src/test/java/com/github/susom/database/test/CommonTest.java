/*
 * Copyright 2014 The Board of Trustees of The Leland Stanford Junior University.
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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.math.BigDecimal;
import java.sql.ResultSetMetaData;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.Month;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

import org.apache.log4j.xml.DOMConfigurator;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.github.susom.database.ConstraintViolationException;
import com.github.susom.database.Database;
import com.github.susom.database.DatabaseException;
import com.github.susom.database.DatabaseProvider;
import com.github.susom.database.OptionsDefault;
import com.github.susom.database.OptionsOverride;
import com.github.susom.database.Row;
import com.github.susom.database.RowHandler;
import com.github.susom.database.Rows;
import com.github.susom.database.RowsHandler;
import com.github.susom.database.Schema;
import com.github.susom.database.Sql;
import com.github.susom.database.SqlArgs;
import com.github.susom.database.StatementAdaptor;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.*;

/**
 * Exercise Database functionality with a real databases.
 *
 * @author garricko
 */
public abstract class CommonTest {
  final static String TEST_TABLE_NAME = "dbtest";

  static {
    // Initialize logging
    String log4jConfig = new File("log4j.xml").getAbsolutePath();
    DOMConfigurator.configure(log4jConfig);
    org.apache.log4j.Logger log = org.apache.log4j.Logger.getLogger(CommonTest.class);
    log.info("Initialized log4j using file: " + log4jConfig);
  }

  protected DatabaseProvider dbp;
  protected Database db;
  protected Date now = new Date();
  protected LocalDate localDateNow = LocalDate.now();

  @Before
  public void setupJdbc() throws Exception {
    dbp = createDatabaseProvider(new OptionsOverride() {
      @Override
      public Date currentDate() {
        return now;
      }

      @Override
      public Calendar calendarForTimestamps() {
        return Calendar.getInstance(TimeZone.getTimeZone("America/Los_Angeles"));
      }
    });
    db = dbp.get();
    db.dropTableQuietly(TEST_TABLE_NAME);
  }

  protected abstract DatabaseProvider createDatabaseProvider(OptionsOverride options) throws Exception;

  @After
  public void closeJdbc() throws Exception {
    if (dbp != null) {
      dbp.commitAndClose();
    }
  }

  @Test
  public void tableExists() {
    // Verify dbtest table does not exist
    String lowercaseTable = TEST_TABLE_NAME.toLowerCase();
    testTableLookup(lowercaseTable);
    db.dropTableQuietly(lowercaseTable);

    // Let's try creating a table with an upper case name and verify it works
    String uppercaseTable = TEST_TABLE_NAME.toUpperCase();
    testTableLookup( uppercaseTable );
    db.dropTableQuietly(uppercaseTable);

    // Verify that null or empty name is handled gracefully
    assertFalse(db.tableExists(null));
    assertFalse(db.tableExists(""));
  }

  private void testTableLookup(String tableName) {
    // Verify test table does not exist
    assertFalse(db.tableExists(tableName));

    // Create and verify it exists.
    new Schema().addTable(tableName).addColumn("pk").primaryKey().schema().execute(db);
    assertTrue(db.tableExists(tableName));
  }

  @Test
  public void normalizeTableName() {
    // Verify that null and empty cases are handled gracefully
    assertNull(db.normalizeTableName(null));
    assertEquals("", db.normalizeTableName(""));

    // Verify a quoted table name is returned in exactly the same case, with quotes removed.
    String camelCaseTableName = "\"DbTest\"";
    assertEquals(camelCaseTableName.substring(1, camelCaseTableName.length()-1),
      db.normalizeTableName(camelCaseTableName));

    // Verify that the database flavor gets the expected normalized case
    boolean isUpperCase = db.flavor().isNormalizedUpperCase();
    if (isUpperCase) {
      assertEquals(TEST_TABLE_NAME.toUpperCase(), db.normalizeTableName(TEST_TABLE_NAME));
    } else {
      assertEquals(TEST_TABLE_NAME.toLowerCase(), db.normalizeTableName(TEST_TABLE_NAME));
    }
  }

  @Test
  public void selectNewTable() {
    new Schema()
        .addTable("dbtest")
          .addColumn("nbr_integer").asInteger().primaryKey().table()
          .addColumn("nbr_long").asLong().table()
          .addColumn("nbr_float").asFloat().table()
          .addColumn("nbr_double").asDouble().table()
          .addColumn("nbr_big_decimal").asBigDecimal(19, 9).table()
          .addColumn("str_varchar").asString(80).table()
          .addColumn("str_fixed").asStringFixed(1).table()
          .addColumn("str_lob").asClob().table()
          .addColumn("bin_blob").asBlob().table()
          .addColumn("date_millis").asDate().table()
          .addColumn("local_date").asLocalDate().table().schema().execute(db);

    BigDecimal bigDecimal = new BigDecimal("5.3");
    db.toInsert("insert into dbtest values (?,?,?,?,?,?,?,?,?,?,?)").argInteger(1).argLong(2L).argFloat(3.2f).argDouble(4.2)
        .argBigDecimal(bigDecimal).argString("Hello").argString("T").argClobString("World")
        .argBlobBytes("More".getBytes()).argDate(now).argLocalDate(localDateNow).insert(1);

    db.toSelect(
        "select nbr_integer, nbr_long, nbr_float, nbr_double, nbr_big_decimal, str_varchar, str_fixed, str_lob, "
            + "bin_blob, date_millis, local_date from dbtest").query(new RowsHandler<Void>() {
      @Override
      public Void process(Rows rs) throws Exception {
        assertTrue(rs.next());
        assertEquals(new Integer(1), rs.getIntegerOrNull(1));
        assertEquals(new Integer(1), rs.getIntegerOrNull("nbr_integer"));
        assertEquals(1, rs.getIntegerOrZero(1));
        assertEquals(1, rs.getIntegerOrZero("nbr_integer"));
        assertEquals(new Long(2), rs.getLongOrNull(2));
        assertEquals(new Long(2), rs.getLongOrNull("nbr_long"));
        assertEquals(2, rs.getLongOrZero(2));
        assertEquals(2, rs.getLongOrZero("nbr_long"));
        assertEquals(new Float(3.2f), rs.getFloatOrNull(3));
        assertEquals(new Float(3.2f), rs.getFloatOrNull("nbr_float"));
        assertEquals(3.2, rs.getFloatOrZero(3), 0.01);
        assertEquals(3.2, rs.getFloatOrZero("nbr_float"), 0.01);
        assertEquals(new Double(4.2), rs.getDoubleOrNull(4));
        assertEquals(new Double(4.2), rs.getDoubleOrNull("nbr_double"));
        assertEquals(4.2, rs.getDoubleOrZero(4), 0.01);
        assertEquals(4.2, rs.getDoubleOrZero("nbr_double"), 0.01);
        assertEquals(new BigDecimal("5.3"), rs.getBigDecimalOrNull(5));
        assertEquals(new BigDecimal("5.3"), rs.getBigDecimalOrNull("nbr_big_decimal"));
        assertEquals(new BigDecimal("5.3"), rs.getBigDecimalOrZero(5));
        assertEquals(new BigDecimal("5.3"), rs.getBigDecimalOrZero("nbr_big_decimal"));
        assertEquals("Hello", rs.getStringOrNull(6));
        assertEquals("Hello", rs.getStringOrNull("str_varchar"));
        assertEquals("Hello", rs.getStringOrEmpty(6));
        assertEquals("Hello", rs.getStringOrEmpty("str_varchar"));
        assertEquals("T", rs.getStringOrNull(7));
        assertEquals("T", rs.getStringOrNull("str_fixed"));
        assertEquals("T", rs.getStringOrEmpty(7));
        assertEquals("T", rs.getStringOrEmpty("str_fixed"));
        assertEquals("World", rs.getClobStringOrNull(8));
        assertEquals("World", rs.getClobStringOrNull("str_lob"));
        assertEquals("World", rs.getClobStringOrEmpty(8));
        assertEquals("World", rs.getClobStringOrEmpty("str_lob"));
        assertArrayEquals("More".getBytes(), rs.getBlobBytesOrNull(9));
        assertArrayEquals("More".getBytes(), rs.getBlobBytesOrNull("bin_blob"));
        assertArrayEquals("More".getBytes(), rs.getBlobBytesOrZeroLen(9));
        assertArrayEquals("More".getBytes(), rs.getBlobBytesOrZeroLen("bin_blob"));
        assertEquals(now, rs.getDateOrNull(10));
        assertEquals(now, rs.getDateOrNull("date_millis"));
        assertEquals(localDateNow, rs.getLocalDateOrNull(11));
        assertEquals(localDateNow, rs.getLocalDateOrNull("local_date"));
        return null;
      }
    });
    // Repeat the above query, using the various methods that automatically infer the column
    db.toSelect(
        "select nbr_integer, nbr_long, nbr_float, nbr_double, nbr_big_decimal, str_varchar, str_fixed, str_lob, "
            + "bin_blob, date_millis, local_date from dbtest").query(new RowsHandler<Void>() {
      @Override
      public Void process(Rows rs) throws Exception {
        assertTrue(rs.next());
        assertEquals(new Integer(1), rs.getIntegerOrNull());
        assertEquals(new Long(2), rs.getLongOrNull());
        assertEquals(new Float(3.2f), rs.getFloatOrNull());
        assertEquals(new Double(4.2), rs.getDoubleOrNull());
        assertEquals(new BigDecimal("5.3"), rs.getBigDecimalOrNull());
        assertEquals("Hello", rs.getStringOrNull());
        assertEquals("T", rs.getStringOrNull());
        assertEquals("World", rs.getClobStringOrNull());
        assertArrayEquals("More".getBytes(), rs.getBlobBytesOrNull());
        assertEquals(now, rs.getDateOrNull());
        assertEquals(localDateNow, rs.getLocalDateOrNull());

        return null;
      }
    });
    db.toSelect(
        "select nbr_integer, nbr_long, nbr_float, nbr_double, nbr_big_decimal, str_varchar, str_fixed, str_lob, "
            + "bin_blob, date_millis, local_date from dbtest").query(new RowsHandler<Void>() {
      @Override
      public Void process(Rows rs) throws Exception {
        assertTrue(rs.next());
        assertEquals(1, rs.getIntegerOrZero());
        assertEquals(2, rs.getLongOrZero());
        assertEquals(3.2, rs.getFloatOrZero(), 0.01);
        assertEquals(4.2, rs.getDoubleOrZero(), 0.01);
        assertEquals(new BigDecimal("5.3"), rs.getBigDecimalOrZero());
        assertEquals("Hello", rs.getStringOrEmpty());
        assertEquals("T", rs.getStringOrEmpty());
        assertEquals("World", rs.getClobStringOrEmpty());
        assertArrayEquals("More".getBytes(), rs.getBlobBytesOrZeroLen());
        return null;
      }
    });
    db.toSelect("select str_lob, bin_blob from dbtest").query(new RowsHandler<Void>() {
      @Override
      public Void process(Rows rs) throws Exception {
        assertTrue(rs.next());
        assertEquals("World", readerToString(rs.getClobReaderOrNull(1)));
        assertArrayEquals("More".getBytes(), inputStreamToString(rs.getBlobInputStreamOrNull(2)));
        return null;
      }
    });
    db.toSelect("select str_lob, bin_blob from dbtest").query(new RowsHandler<Void>() {
      @Override
      public Void process(Rows rs) throws Exception {
        assertTrue(rs.next());
        assertEquals("World", readerToString(rs.getClobReaderOrEmpty(1)));
        assertArrayEquals("More".getBytes(), inputStreamToString(rs.getBlobInputStreamOrEmpty(2)));
        return null;
      }
    });
    db.toSelect("select str_lob, bin_blob from dbtest").query(new RowsHandler<Void>() {
      @Override
      public Void process(Rows rs) throws Exception {
        assertTrue(rs.next());
        assertEquals("World", readerToString(rs.getClobReaderOrNull()));
        assertArrayEquals("More".getBytes(), inputStreamToString(rs.getBlobInputStreamOrNull()));
        return null;
      }
    });
    db.toSelect("select str_lob, bin_blob from dbtest").query(new RowsHandler<Void>() {
      @Override
      public Void process(Rows rs) throws Exception {
        assertTrue(rs.next());
        assertEquals("World", readerToString(rs.getClobReaderOrEmpty()));
        assertArrayEquals("More".getBytes(), inputStreamToString(rs.getBlobInputStreamOrEmpty()));
        return null;
      }
    });
    db.toSelect("select str_lob, bin_blob from dbtest").query(new RowsHandler<Void>() {
      @Override
      public Void process(Rows rs) throws Exception {
        assertTrue(rs.next());
        assertEquals("World", readerToString(rs.getClobReaderOrNull("str_lob")));
        assertArrayEquals("More".getBytes(), inputStreamToString(rs.getBlobInputStreamOrNull("bin_blob")));
        return null;
      }
    });
    db.toSelect("select str_lob, bin_blob from dbtest").query(new RowsHandler<Void>() {
      @Override
      public Void process(Rows rs) throws Exception {
        assertTrue(rs.next());
        assertEquals("World", readerToString(rs.getClobReaderOrEmpty("str_lob")));
        assertArrayEquals("More".getBytes(), inputStreamToString(rs.getBlobInputStreamOrEmpty("bin_blob")));
        return null;
      }
    });

    assertEquals(new Long(1), db.toSelect("select count(*) from dbtest where nbr_integer=:i and nbr_long=:l and "
        + "abs(nbr_float-:f)<0.01 and abs(nbr_double-:d)<0.01 and nbr_big_decimal=:bd and str_varchar=:s "
        + "and str_fixed=:sf and date_millis=:date and local_date=:local_date").argInteger("i", 1).argLong("l", 2L).argFloat("f", 3.2f)
        .argDouble("d", 4.2).argBigDecimal("bd", bigDecimal).argString("s", "Hello").argString("sf", "T")
        .argDate("date", now).argLocalDate( "local_date", localDateNow).queryLongOrNull());
    List<Long> result = db.toSelect("select count(*) from dbtest where nbr_integer=:i and nbr_long=:l and "
        + "abs(nbr_float-:f)<0.01 and abs(nbr_double-:d)<0.01 and nbr_big_decimal=:bd and str_varchar=:s "
        + "and str_fixed=:sf and date_millis=:date and local_date=:local_date").argInteger("i", 1).argLong("l", 2L).argFloat("f", 3.2f)
        .argDouble("d", 4.2).argBigDecimal("bd", bigDecimal).argString("s", "Hello").argString("sf", "T")
        .argDate("date", now).argLocalDate("local_date", localDateNow).queryLongs();
    assertEquals(1, result.size());
    assertEquals(new Long(1), result.get(0));
  }

  @Test
  public void updatePositionalArgs() {
    new Schema()
        .addTable("dbtest")
        .addColumn("pk").primaryKey().table()
        .addColumn("nbr_integer").asInteger().table()
        .addColumn("nbr_long").asLong().table()
        .addColumn("nbr_float").asFloat().table()
        .addColumn("nbr_double").asDouble().table()
        .addColumn("nbr_big_decimal").asBigDecimal(19, 9).table()
        .addColumn("str_varchar").asString(80).table()
        .addColumn("str_fixed").asStringFixed(1).table()
        .addColumn("str_lob").asClob().table()
        .addColumn("bin_blob").asBlob().table()
        .addColumn("date_millis").asDate().table()
        .addColumn("local_date").asLocalDate().table().schema().execute(db);

    BigDecimal bigDecimal = new BigDecimal("5.3");
    assertEquals(1, db.toInsert("insert into dbtest values (?,?,?,?,?,?,?,?,?,?,?,?)")
        .argLong(1L)
        .argInteger(1)
        .argLong(2L)
        .argFloat(3.2f)
        .argDouble(4.2)
        .argBigDecimal(bigDecimal)
        .argString("Hello")
        .argString("T")
        .argClobString("World")
        .argBlobBytes("More".getBytes())
        .argDate(now)
        .argLocalDate(localDateNow).insert());

    db.toUpdate("update dbtest set nbr_integer=?, nbr_long=?, nbr_float=?, nbr_double=?, nbr_big_decimal=?, "
        + "str_varchar=?, str_fixed=?, str_lob=?, bin_blob=?, date_millis=?, local_date=?").argInteger(null).argLong(null)
        .argFloat(null).argDouble(null).argBigDecimal(null).argString(null).argString(null).argClobString(null)
        .argBlobBytes(null).argDate(null).argLocalDate(null).update(1);
    db.toSelect(
        "select nbr_integer, nbr_long, nbr_float, nbr_double, nbr_big_decimal, str_varchar, str_fixed, str_lob, "
            + "bin_blob, date_millis, local_date from dbtest").query(new RowsHandler<Void>() {
      @Override
      public Void process(Rows rs) throws Exception {
        assertTrue(rs.next());
        assertNull(rs.getIntegerOrNull(1));
        assertNull(rs.getIntegerOrNull("nbr_integer"));
        assertNull(rs.getLongOrNull(2));
        assertNull(rs.getLongOrNull("nbr_long"));
        assertNull(rs.getFloatOrNull(3));
        assertNull(rs.getFloatOrNull("nbr_float"));
        assertNull(rs.getDoubleOrNull(4));
        assertNull(rs.getDoubleOrNull("nbr_double"));
        assertNull(rs.getBigDecimalOrNull(5));
        assertNull(rs.getBigDecimalOrNull("nbr_big_decimal"));
        assertNull(rs.getStringOrNull(6));
        assertNull(rs.getStringOrNull("str_varchar"));
        assertNull(rs.getStringOrNull(7));
        assertNull(rs.getStringOrNull("str_fixed"));
        assertNull(rs.getClobStringOrNull(8));
        assertNull(rs.getClobStringOrNull("str_lob"));
        assertNull(rs.getBlobBytesOrNull(9));
        assertNull(rs.getBlobBytesOrNull("bin_blob"));
        assertNull(rs.getDateOrNull(10));
        assertNull(rs.getDateOrNull("date_millis"));
        assertNull(rs.getLocalDateOrNull(11));
        assertNull(rs.getLocalDateOrNull("local_date"));
        return null;
      }
    });
    assertEquals(1, db.toUpdate("update dbtest set nbr_integer=?, nbr_long=?, nbr_float=?, nbr_double=?, "
        + "nbr_big_decimal=?, str_varchar=?, str_fixed=?, str_lob=?, bin_blob=?, date_millis=?, local_date=?").argInteger(1)
        .argLong(2L).argFloat(3.2f).argDouble(4.2).argBigDecimal(bigDecimal).argString("Hello").argString("T")
        .argClobString("World").argBlobBytes("More".getBytes()).argDate(now).argLocalDate(localDateNow).update());
    db.toSelect(
        "select nbr_integer, nbr_long, nbr_float, nbr_double, nbr_big_decimal, str_varchar, str_fixed, str_lob, "
            + "bin_blob, date_millis, local_date from dbtest").query(new RowsHandler<Void>() {
      @Override
      public Void process(Rows rs) throws Exception {
        assertTrue(rs.next());
        assertEquals(new Integer(1), rs.getIntegerOrNull(1));
        assertEquals(new Integer(1), rs.getIntegerOrNull("nbr_integer"));
        assertEquals(new Long(2), rs.getLongOrNull(2));
        assertEquals(new Long(2), rs.getLongOrNull("nbr_long"));
        assertEquals(new Float(3.2f), rs.getFloatOrNull(3));
        assertEquals(new Float(3.2f), rs.getFloatOrNull("nbr_float"));
        assertEquals(new Double(4.2), rs.getDoubleOrNull(4));
        assertEquals(new Double(4.2), rs.getDoubleOrNull("nbr_double"));
        assertEquals(new BigDecimal("5.3"), rs.getBigDecimalOrNull(5));
        assertEquals(new BigDecimal("5.3"), rs.getBigDecimalOrNull("nbr_big_decimal"));
        assertEquals("Hello", rs.getStringOrNull(6));
        assertEquals("Hello", rs.getStringOrNull("str_varchar"));
        assertEquals("T", rs.getStringOrNull(7));
        assertEquals("T", rs.getStringOrNull("str_fixed"));
        assertEquals("World", rs.getClobStringOrNull(8));
        assertEquals("World", rs.getClobStringOrNull("str_lob"));
        assertArrayEquals("More".getBytes(), rs.getBlobBytesOrNull(9));
        assertArrayEquals("More".getBytes(), rs.getBlobBytesOrNull("bin_blob"));
        assertEquals(now, rs.getDateOrNull(10));
        assertEquals(now, rs.getDateOrNull("date_millis"));
        assertEquals(localDateNow, rs.getLocalDateOrNull(11));
        assertEquals(localDateNow, rs.getLocalDateOrNull("local_date"));
        return null;
      }
    });

    db.toUpdate("update dbtest set str_lob=?, bin_blob=?").argClobReader(null).argBlobStream(null).update(1);
    db.toSelect("select str_lob, bin_blob from dbtest").query(new RowsHandler<Void>() {
      @Override
      public Void process(Rows rs) throws Exception {
        assertTrue(rs.next());
        assertNull(rs.getClobStringOrNull(1));
        assertNull(rs.getClobStringOrNull("str_lob"));
        assertNull(rs.getBlobBytesOrNull(2));
        assertNull(rs.getBlobBytesOrNull("bin_blob"));
        return null;
      }
    });
    db.toUpdate("update dbtest set str_lob=?, bin_blob=?").argClobReader(new StringReader("World"))
        .argBlobStream(new ByteArrayInputStream("More".getBytes())).update(1);
    db.toSelect("select str_lob, bin_blob from dbtest").query(new RowsHandler<Void>() {
      @Override
      public Void process(Rows rs) throws Exception {
        assertTrue(rs.next());
        assertEquals("World", rs.getClobStringOrNull(1));
        assertEquals("World", rs.getClobStringOrNull("str_lob"));
        assertArrayEquals("More".getBytes(), rs.getBlobBytesOrNull(2));
        assertArrayEquals("More".getBytes(), rs.getBlobBytesOrNull("bin_blob"));
        return null;
      }
    });
  }

  @Test
  public void updateNamedArgs() {
    new Schema()
        .addTable("dbtest")
        .addColumn("pk").primaryKey().table()
        .addColumn("nbr_integer").asInteger().table()
        .addColumn("nbr_long").asLong().table()
        .addColumn("nbr_float").asFloat().table()
        .addColumn("nbr_double").asDouble().table()
        .addColumn("nbr_big_decimal").asBigDecimal(19, 9).table()
        .addColumn("str_varchar").asString(80).table()
        .addColumn("str_fixed").asStringFixed(1).table()
        .addColumn("str_lob").asClob().table()
        .addColumn("bin_blob").asBlob().table()
        .addColumn("date_millis").asDate().table()
        .addColumn("local_date").asLocalDate().table().schema().execute(db);

    BigDecimal bigDecimal = new BigDecimal("5.3");
    db.toInsert("insert into dbtest values (:pk,:a,:b,:c,:d,:e,:f,:sf,:g,:h,:i,:j)").argLong(":pk", 1L).argInteger(":a", 1)
        .argLong(":b", 2L).argFloat(":c", 3.2f).argDouble(":d", 4.2).argBigDecimal(":e", bigDecimal)
        .argString(":f", "Hello").argString(":sf", "T")
        .argClobString(":g", "World").argBlobBytes(":h", "More".getBytes())
        .argDate(":i", now).argLocalDate(":j", localDateNow).insert(1);
    db.toUpdate("update dbtest set nbr_integer=:a, nbr_long=:b, nbr_float=:c, nbr_double=:d, nbr_big_decimal=:e, "
        + "str_varchar=:f, str_fixed=:sf, str_lob=:g, bin_blob=:h, date_millis=:i, local_date=:j").argInteger(":a", null)
        .argLong(":b", null).argFloat(":c", null).argDouble(":d", null).argBigDecimal(":e", null)
        .argString(":f", null).argString(":sf", null)
        .argClobString(":g", null).argBlobBytes(":h", null)
        .argDate(":i", null).argLocalDate(":j", null).update(1);
    db.toSelect(
        "select nbr_integer, nbr_long, nbr_float, nbr_double, nbr_big_decimal, str_varchar, str_fixed, str_lob, "
            + "bin_blob, date_millis, local_date from dbtest").query(new RowsHandler<Void>() {
      @Override
      public Void process(Rows rs) throws Exception {
        assertTrue(rs.next());
        assertNull(rs.getIntegerOrNull(1));
        assertNull(rs.getIntegerOrNull("nbr_integer"));
        assertNull(rs.getLongOrNull(2));
        assertNull(rs.getLongOrNull("nbr_long"));
        assertNull(rs.getFloatOrNull(3));
        assertNull(rs.getFloatOrNull("nbr_float"));
        assertNull(rs.getDoubleOrNull(4));
        assertNull(rs.getDoubleOrNull("nbr_double"));
        assertNull(rs.getBigDecimalOrNull(5));
        assertNull(rs.getBigDecimalOrNull("nbr_big_decimal"));
        assertNull(rs.getStringOrNull(6));
        assertNull(rs.getStringOrNull("str_varchar"));
        assertNull(rs.getStringOrNull(7));
        assertNull(rs.getStringOrNull("str_fixed"));
        assertNull(rs.getClobStringOrNull(8));
        assertNull(rs.getClobStringOrNull("str_lob"));
        assertNull(rs.getBlobBytesOrNull(9));
        assertNull(rs.getBlobBytesOrNull("bin_blob"));
        assertNull(rs.getDateOrNull(10));
        assertNull(rs.getDateOrNull("date_millis"));
        assertNull(rs.getLocalDateOrNull(11));
        assertNull(rs.getLocalDateOrNull("local_date"));
        return null;
      }
    });
    db.toUpdate("update dbtest set nbr_integer=:a, nbr_long=:b, nbr_float=:c, nbr_double=:d, nbr_big_decimal=:e, "
        + "str_varchar=:f, str_fixed=:sf, str_lob=:g, bin_blob=:h, date_millis=:i, local_date=:j").argInteger(":a", 1)
        .argLong(":b", 2L).argFloat(":c", 3.2f).argDouble(":d", 4.2).argBigDecimal(":e", bigDecimal)
        .argString(":f", "Hello").argString(":sf", "T")
        .argClobString(":g", "World").argBlobBytes(":h", "More".getBytes())
        .argDate(":i", now).argLocalDate(":j", localDateNow).update(1);
    db.toSelect(
        "select nbr_integer, nbr_long, nbr_float, nbr_double, nbr_big_decimal, str_varchar, str_fixed, str_lob, "
            + "bin_blob, date_millis, local_date from dbtest").query(new RowsHandler<Void>() {
      @Override
      public Void process(Rows rs) throws Exception {
        assertTrue(rs.next());
        assertEquals(new Integer(1), rs.getIntegerOrNull(1));
        assertEquals(new Integer(1), rs.getIntegerOrNull("nbr_integer"));
        assertEquals(new Long(2), rs.getLongOrNull(2));
        assertEquals(new Long(2), rs.getLongOrNull("nbr_long"));
        assertEquals(new Float(3.2f), rs.getFloatOrNull(3));
        assertEquals(new Float(3.2f), rs.getFloatOrNull("nbr_float"));
        assertEquals(new Double(4.2), rs.getDoubleOrNull(4));
        assertEquals(new Double(4.2), rs.getDoubleOrNull("nbr_double"));
        assertEquals(new BigDecimal("5.3"), rs.getBigDecimalOrNull(5));
        assertEquals(new BigDecimal("5.3"), rs.getBigDecimalOrNull("nbr_big_decimal"));
        assertEquals("Hello", rs.getStringOrNull(6));
        assertEquals("Hello", rs.getStringOrNull("str_varchar"));
        assertEquals("T", rs.getStringOrNull(7));
        assertEquals("T", rs.getStringOrNull("str_fixed"));
        assertEquals("World", rs.getClobStringOrNull(8));
        assertEquals("World", rs.getClobStringOrNull("str_lob"));
        assertArrayEquals("More".getBytes(), rs.getBlobBytesOrNull(9));
        assertArrayEquals("More".getBytes(), rs.getBlobBytesOrNull("bin_blob"));
        assertEquals(now, rs.getDateOrNull(10));
        assertEquals(now, rs.getDateOrNull("date_millis"));
        assertEquals(localDateNow, rs.getLocalDateOrNull(11));
        assertEquals(localDateNow, rs.getLocalDateOrNull("local_date"));
        return null;
      }
    });

    db.toUpdate("update dbtest set str_lob=:a, bin_blob=:b").argClobReader(":a", null).argBlobStream(":b", null).update(1);
    db.toSelect("select str_lob, bin_blob from dbtest").query(new RowsHandler<Void>() {
      @Override
      public Void process(Rows rs) throws Exception {
        assertTrue(rs.next());
        assertNull(rs.getClobStringOrNull(1));
        assertNull(rs.getClobStringOrNull("str_lob"));
        assertNull(rs.getBlobBytesOrNull(2));
        assertNull(rs.getBlobBytesOrNull("bin_blob"));
        return null;
      }
    });
    db.toUpdate("update dbtest set str_lob=:a, bin_blob=:b").argClobReader(":a", new StringReader("World"))
        .argBlobStream(":b", new ByteArrayInputStream("More".getBytes())).update(1);
    db.toSelect("select str_lob, bin_blob from dbtest").query(new RowsHandler<Void>() {
      @Override
      public Void process(Rows rs) throws Exception {
        assertTrue(rs.next());
        assertEquals("World", rs.getClobStringOrNull(1));
        assertEquals("World", rs.getClobStringOrNull("str_lob"));
        assertArrayEquals("More".getBytes(), rs.getBlobBytesOrNull(2));
        assertArrayEquals("More".getBytes(), rs.getBlobBytesOrNull("bin_blob"));
        return null;
      }
    });
  }

  @Test
  public void nullValues() {
    new Schema()
        .addTable("dbtest")
        .addColumn("pk").primaryKey().table()
        .addColumn("nbr_integer").asInteger().table()
        .addColumn("nbr_long").asLong().table()
        .addColumn("nbr_float").asFloat().table()
        .addColumn("nbr_double").asDouble().table()
        .addColumn("nbr_big_decimal").asBigDecimal(19, 9).table()
        .addColumn("str_varchar").asString(80).table()
        .addColumn("str_fixed").asStringFixed(1).table()
        .addColumn("str_lob").asClob().table()
        .addColumn("bin_blob").asBlob().table()
        .addColumn("date_millis").asDate().table()
        .addColumn("local_date").asLocalDate().table().schema().execute(db);

    db.toInsert("insert into dbtest values (?,?,?,?,?,?,?,?,?,?,?,?)").argLong(1L).argInteger(null).argLong(null)
        .argFloat(null).argDouble(null).argBigDecimal(null).argString(null).argString(null).argClobString(null)
        .argBlobBytes(null).argDate(null).argLocalDate(null).insert(1);
    db.toSelect(
        "select nbr_integer, nbr_long, nbr_float, nbr_double, nbr_big_decimal, str_varchar, str_fixed, str_lob, "
            + "bin_blob, date_millis, local_date from dbtest").query(new RowsHandler<Void>() {
      @Override
      public Void process(Rows rs) throws Exception {
        assertTrue(rs.next());
        assertNull(rs.getIntegerOrNull(1));
        assertNull(rs.getIntegerOrNull("nbr_integer"));
        assertNull(rs.getLongOrNull(2));
        assertNull(rs.getLongOrNull("nbr_long"));
        assertNull(rs.getFloatOrNull(3));
        assertNull(rs.getFloatOrNull("nbr_float"));
        assertNull(rs.getDoubleOrNull(4));
        assertNull(rs.getDoubleOrNull("nbr_double"));
        assertNull(rs.getBigDecimalOrNull(5));
        assertNull(rs.getBigDecimalOrNull("nbr_big_decimal"));
        assertNull(rs.getStringOrNull(6));
        assertNull(rs.getStringOrNull("str_varchar"));
        assertNull(rs.getStringOrNull(7));
        assertNull(rs.getStringOrNull("str_fixed"));
        assertNull(rs.getClobStringOrNull(8));
        assertNull(rs.getClobStringOrNull("str_lob"));
        assertNull(rs.getBlobBytesOrNull(9));
        assertNull(rs.getBlobBytesOrNull("bin_blob"));
        assertNull(rs.getDateOrNull(10));
        assertNull(rs.getDateOrNull("date_millis"));
        assertNull(rs.getLocalDateOrNull(11));
        assertNull(rs.getLocalDateOrNull("local_date"));
        return null;
      }
    });
    db.toSelect("select str_lob, bin_blob from dbtest").query(new RowsHandler<Void>() {
      @Override
      public Void process(Rows rs) throws Exception {
        assertTrue(rs.next());
        assertNull(rs.getClobReaderOrNull(1));
        assertNull(rs.getBlobInputStreamOrNull(2));
        return null;
      }
    });
    db.toSelect("select str_lob, bin_blob from dbtest").query(new RowsHandler<Void>() {
      @Override
      public Void process(Rows rs) throws Exception {
        assertTrue(rs.next());
        assertNull(rs.getClobReaderOrNull("str_lob"));
        assertNull(rs.getBlobInputStreamOrNull("bin_blob"));
        return null;
      }
    });
  }

  @Test
  public void fromAny() {
    assertEquals(db.toSelect("select 1" + db.flavor().fromAny()).queryIntegerOrZero(), 1);
  }

  @Test
  public void metadataColumnNames() {
    new Schema().addTable("dbtest").addColumn("pk").primaryKey().schema().execute(db);

    db.toSelect("select Pk, Pk as Foo, Pk as \"Foo\" from dbtest").query(rs -> {
      assertArrayEquals(new String[] { "PK", "FOO", "Foo" }, rs.getColumnLabels());
      return null;
    });
  }

  @Test
  public void metadataColumnTypes() {
    String timestampColumnName = "data_millis";
    String dateColumnName = "local_date";
    new Schema()
        .addTable("dbtest")
        .addColumn(timestampColumnName).asDate().table()
        .addColumn(dateColumnName).asLocalDate().table().schema().execute(db);
    db.toSelect("select * from dbtest").query(new RowsHandler<Void>() {
      @Override
      public Void process(Rows rs) throws Exception {

        ResultSetMetaData metadata = rs.getMetadata();
        for (int i=1; i <= metadata.getColumnCount(); i++) {
          String columnName = metadata.getColumnName(i);
          String columnType = metadata.getColumnTypeName(i);

          if (columnName.equalsIgnoreCase(timestampColumnName)) {
            if ("sqlserver".equals(db.flavor().toString())) {
              assertEquals("DATETIME2", columnType.toUpperCase());
            } else{
              assertEquals("TIMESTAMP", columnType.toUpperCase());
            }
          } else if (columnName.equalsIgnoreCase(dateColumnName)) {
            assertEquals("DATE", columnType.toUpperCase());
          } else {
            fail("Unexpected column " + columnName + " of type " + columnType);
          }
        }
        return null;
      }
    });
  }

  @Test
  public void intervals() {
    new Schema().addTable("dbtest").addColumn("d").asDate().schema().execute(db);

    db.toInsert("insert into dbtest (d) values (?)").argDate(now).insert(1);

    assertEquals(1, db.toSelect("select count(1) from dbtest where d - interval '1' hour * ? < ?")
        .argInteger(2)
        .argDate(now)
        .queryIntegerOrZero());
  }

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
        .argInteger(Integer.MAX_VALUE).argLong(Long.MAX_VALUE).argFloat(Float.MAX_VALUE)
        .argDouble(Double.MAX_VALUE).argBigDecimal(new BigDecimal("123.456"))
        .argString("hello").argString("Z").argClobString("hello again")
        .argBlobBytes(new byte[] { '1', '2' }).argBoolean(true)
        .argDateNowPerApp().argLocalDate(localDateNow).insert(1);

    db.toInsert("insert into dbtest (nbr_integer, nbr_long, nbr_float, nbr_double, nbr_big_decimal, str_varchar,"
            + " str_fixed, str_lob, bin_blob, boolean_flag, date_millis, local_date) values (?,?,?,?,?,?,?,?,?,?,?,?)")
        .argInteger(Integer.MIN_VALUE).argLong(Long.MIN_VALUE).argFloat(0.000001f)
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

    assertEquals(
        db.toSelect("select nbr_integer, nbr_long, nbr_float, nbr_double, nbr_big_decimal,"
          + " str_varchar, str_fixed, str_lob, bin_blob, boolean_flag, date_millis, local_date from dbtest order by 1")
          .queryMany(SqlArgs::readRow),
        db.toSelect("select nbr_integer, nbr_long, nbr_float, nbr_double, nbr_big_decimal,"
          + " str_varchar, str_fixed, str_lob, bin_blob, boolean_flag, date_millis, local_date from dbtest2 order by 1")
        .  queryMany(SqlArgs::readRow));

    assertEquals(
        Arrays.asList(
            new SqlArgs().argInteger("nbr_integer", Integer.MIN_VALUE)
                    .argLong("nbr_long", Long.MIN_VALUE)
                    .argFloat("nbr_float", 0.000001f)
                    .argDouble("nbr_double", Double.MIN_VALUE)
                    .argBigDecimal("nbr_big_decimal", new BigDecimal("-123.456"))
                    .argString("str_varchar", "goodbye")
                    .argString("str_fixed", "A")
                    .argClobString("str_lob", "bye again")
                    .argBlobBytes("bin_blob", new byte[] { '3', '4' })
                    .argString("boolean_flag", "N")//.argBoolean("boolean_flag", false)
                    .argDate("date_millis", now)
                    .argLocalDate("local_date", localDateNow),
            new SqlArgs().argInteger("nbr_integer", Integer.MAX_VALUE)
                    .argLong("nbr_long", Long.MAX_VALUE)
                    .argFloat("nbr_float", Float.MAX_VALUE)
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
  public void readSqlArgs() {
    new Schema().addTable("dbtest").addColumn("pk").primaryKey().schema().execute(db);

    db.toInsert("insert into dbtest (pk) values (?)").argInteger(1).insert(1);

    SqlArgs args = db.toSelect("select Pk, Pk as Foo, Pk as \"Foo\", pk as \"g arB#G!\","
        + " pk as \"TitleCase\" from dbtest")
        .queryOneOrThrow(SqlArgs::readRow);

    assertEquals(Arrays.asList("pk", "foo", "foo_2", "g_ar_b_g", "title_case"), args.names());
  }

  @Test
  public void clockSync() {
    db.assertTimeSynchronized();
  }

  @Test
  public void booleanColumn() {
    new Schema().addTable("dbtest")
        .addColumn("t").asBoolean().table()
        .addColumn("f").asBoolean().table()
        .addColumn("n").asBoolean().schema().execute(db);

    db.toInsert("insert into dbtest (t,f,n) values (?,:f,?)")
        .argBoolean(true).argBoolean("f", false).argBoolean(null).insert(1);
    db.toSelect("select t,f,n from dbtest")
        .query(new RowsHandler<Object>() {
          @Override
          public Object process(Rows rs) throws Exception {
            assertTrue(rs.next());
            assertTrue(rs.getBooleanOrNull() == Boolean.TRUE);
            assertTrue(rs.getBooleanOrNull() == Boolean.FALSE);
            assertNull(rs.getBooleanOrNull());
            return null;
          }
        });
    // Verify use of getBooleanOrNull(int) followed by default getBooleanOrNull() tracks
    // the current column index correctly (picks up where the explicit one left off)
    db.toSelect("select t,f,n from dbtest")
        .query(new RowsHandler<Object>() {
          @Override
          public Object process(Rows rs) throws Exception {
            assertTrue(rs.next());
            assertTrue(rs.getBooleanOrNull(2) == Boolean.FALSE);
            assertNull(rs.getBooleanOrNull());
            return null;
          }
        });
    // Verify use of getBooleanOrNull(String) followed by default getBooleanOrNull() tracks
    // the current column index correctly (picks up where the explicit one left off)
    db.toSelect("select t,f,n from dbtest")
        .query(new RowsHandler<Object>() {
          @Override
          public Object process(Rows rs) throws Exception {
            assertTrue(rs.next());
            assertTrue(rs.getBooleanOrNull("f") == Boolean.FALSE);
            assertNull(rs.getBooleanOrNull());
            return null;
          }
        });
    db.toSelect("select t,f,n from dbtest")
        .query(new RowsHandler<Object>() {
          @Override
          public Object process(Rows rs) throws Exception {
            assertTrue(rs.next());
            assertTrue(rs.getBooleanOrFalse());
            assertFalse(rs.getBooleanOrFalse());
            assertFalse(rs.getBooleanOrFalse());
            return null;
          }
        });
    // Verify use of getBooleanOrFalse(int) followed by default getBooleanOrFalse() tracks
    // the current column index correctly (picks up where the explicit one left off)
    db.toSelect("select t,f,n from dbtest")
        .query(new RowsHandler<Object>() {
          @Override
          public Object process(Rows rs) throws Exception {
            assertTrue(rs.next());
            assertFalse(rs.getBooleanOrFalse(2));
            assertFalse(rs.getBooleanOrFalse());
            return null;
          }
        });
    // Verify use of getBooleanOrFalse(String) followed by default getBooleanOrFalse() tracks
    // the current column index correctly (picks up where the explicit one left off)
    db.toSelect("select t,f,n from dbtest")
        .query(new RowsHandler<Object>() {
          @Override
          public Object process(Rows rs) throws Exception {
            assertTrue(rs.next());
            assertFalse(rs.getBooleanOrFalse("f"));
            assertFalse(rs.getBooleanOrFalse());
            return null;
          }
        });
    db.toSelect("select t,f,n from dbtest")
        .query(new RowsHandler<Object>() {
          @Override
          public Object process(Rows rs) throws Exception {
            assertTrue(rs.next());
            assertTrue(rs.getBooleanOrTrue());
            assertFalse(rs.getBooleanOrTrue());
            assertTrue(rs.getBooleanOrTrue());
            return null;
          }
        });
    // Verify use of getBooleanOrTrue(int) followed by default getBooleanOrTrue() tracks
    // the current column index correctly (picks up where the explicit one left off)
    db.toSelect("select t,f,n from dbtest")
        .query(new RowsHandler<Object>() {
          @Override
          public Object process(Rows rs) throws Exception {
            assertTrue(rs.next());
            assertFalse(rs.getBooleanOrTrue(2));
            assertTrue(rs.getBooleanOrTrue());
            return null;
          }
        });
    // Verify use of getBooleanOrTrue(String) followed by default getBooleanOrTrue() tracks
    // the current column index correctly (picks up where the explicit one left off)
    db.toSelect("select t,f,n from dbtest")
        .query(new RowsHandler<Object>() {
          @Override
          public Object process(Rows rs) throws Exception {
            assertTrue(rs.next());
            assertFalse(rs.getBooleanOrTrue("f"));
            assertTrue(rs.getBooleanOrTrue());
            return null;
          }
        });
    db.toDelete("delete from dbtest where t=? and f=?")
        .argBoolean(true).argBoolean(false).update(1);
    // Really should do this, but it seems Derby and PostgreSQL don't support it
//    db.toDelete("delete from dbtest where t=? and f=? and n=?")
//    .argBoolean(true).argBoolean(false).argBoolean(null).update(1);

    db.toInsert("insert into dbtest (t,f,n) values (:t,:f,:n)")
        .argBoolean("t", true).argBoolean("f", false).argBoolean("n", null).insert(1);
    db.toSelect("select t,f,n from dbtest")
        .query(new RowsHandler<Object>() {
          @Override
          public Object process(Rows rs) throws Exception {
            assertTrue(rs.next());
            assertTrue(rs.getBooleanOrNull(1) == Boolean.TRUE);
            assertTrue(rs.getBooleanOrNull(2) == Boolean.FALSE);
            assertNull(rs.getBooleanOrNull(3));
            assertTrue(rs.getBooleanOrFalse(1) == Boolean.TRUE);
            assertTrue(rs.getBooleanOrFalse(2) == Boolean.FALSE);
            assertTrue(rs.getBooleanOrFalse(3) == Boolean.FALSE);
            assertTrue(rs.getBooleanOrTrue(1) == Boolean.TRUE);
            assertTrue(rs.getBooleanOrTrue(2) == Boolean.FALSE);
            assertTrue(rs.getBooleanOrTrue(3) == Boolean.TRUE);
            assertTrue(rs.getBooleanOrNull("t") == Boolean.TRUE);
            assertTrue(rs.getBooleanOrNull("f") == Boolean.FALSE);
            assertNull(rs.getBooleanOrNull("n"));
            assertTrue(rs.getBooleanOrFalse("t") == Boolean.TRUE);
            assertTrue(rs.getBooleanOrFalse("f") == Boolean.FALSE);
            assertTrue(rs.getBooleanOrFalse("n") == Boolean.FALSE);
            assertTrue(rs.getBooleanOrTrue("t") == Boolean.TRUE);
            assertTrue(rs.getBooleanOrTrue("f") == Boolean.FALSE);
            assertTrue(rs.getBooleanOrTrue("n") == Boolean.TRUE);
            return null;
          }
        });
    assertTrue(db.toSelect("select t from dbtest").queryBooleanOrNull() == Boolean.TRUE);
    assertTrue(db.toSelect("select t from dbtest").queryBooleanOrFalse());
    assertTrue(db.toSelect("select t from dbtest").queryBooleanOrTrue());
    assertTrue(db.toSelect("select f from dbtest").queryBooleanOrNull() == Boolean.FALSE);
    assertFalse(db.toSelect("select f from dbtest").queryBooleanOrFalse());
    assertFalse(db.toSelect("select f from dbtest").queryBooleanOrTrue());
    assertNull(db.toSelect("select n from dbtest").queryBooleanOrNull());
    assertFalse(db.toSelect("select n from dbtest").queryBooleanOrFalse());
    assertTrue(db.toSelect("select n from dbtest").queryBooleanOrTrue());
  }

  @Test
  public void batchInsert() {
    new Schema().addTable("dbtest")
        .addColumn("pk").primaryKey().schema().execute(db);

    db.toInsert("insert into dbtest (pk) values (?)")
        .argInteger(1).batch()
        .argInteger(2).batch()
        .argInteger(3).batch().insertBatch();

    assertEquals(3, db.toSelect("select count(*) from dbtest").queryIntegerOrZero());
  }

  @Test
  public void batchInsertPkLong() {
    new Schema().addTable("dbtest")
        .addColumn("pk").primaryKey().table()
        .addColumn("s").asString(10).schema().execute(db);

    db.toInsert("insert into dbtest (pk,s) values (?,?)")
        .argPkLong(1L).argString("hi").batch()
        .argPkLong(2L).argString("hello").batch()
        .argPkLong(3L).argString("howdy").batch().insertBatch();

    assertEquals(3, db.toSelect("select count(*) from dbtest").queryIntegerOrZero());

    try {
      db.toInsert("insert into dbtest (pk,s) values (?,?)")
          .argPkLong(1L).argString("hi").batch()
          // argPkLong in different position ==> error
          .argString("hello").argPkLong(2L).batch().insertBatch();
      fail("Expecting an exception to be thrown");
    } catch (DatabaseException e) {
      assertEquals("The argPkLong() calls must be in the same position across batch records", e.getMessage());
    }

    try {
      db.toInsert("insert into dbtest (pk,s) values (?,?)")
          // multiple pk calls ==> error
          .argPkLong(1L).argPkLong(1L).batch()
          .argPkLong(2L).argString("hello").batch().insertBatch();
      fail("Expecting an exception to be thrown");
    } catch (DatabaseException e) {
      assertEquals("Only call one argPk*() method", e.getMessage());
    }
  }

  @Test
  public void batchInsertPkLongNamed() {
    new Schema().addTable("dbtest")
        .addColumn("pk").primaryKey().table()
        .addColumn("s").asString(10).schema().execute(db);

    db.toInsert("insert into dbtest (pk,s) values (:pk,?)")
        .argPkLong("pk", 1L).argString("hi").batch()
        .argPkLong("pk", 2L).argString("hello").batch()
        .argPkLong("pk", 3L).argString("howdy").batch().insertBatch();

    assertEquals(3, db.toSelect("select count(*) from dbtest").queryIntegerOrZero());

    db.toInsert("insert into dbtest (pk,s) values (:pk,?)")
        .batch().argPkLong("pk", 4L).argString("hi").batch()
        .argString("hello").argPkLong("pk", 5L).insertBatch();

    assertEquals(5, db.toSelect("select count(*) from dbtest").queryIntegerOrZero());

    try {
      db.toInsert("insert into dbtest (pk,s) values (:pk,?)")
          // multiple pk calls ==> error
          .argPkLong("pk", 1L).argPkLong(1L).batch().insertBatch();
      fail("Expecting an exception to be thrown");
    } catch (DatabaseException e) {
      assertEquals("Only call one argPk*() method", e.getMessage());
    }

    try {
      db.toInsert("insert into dbtest (pk,s) values (?,?)")
          .argPkLong("pk", 1L).argString("howdy").batch()
          // different name for pk on second batch ==> error
          .argPkLong("na", 2L).argString("hello").batch().insertBatch();
      fail("Expecting an exception to be thrown");
    } catch (DatabaseException e) {
      assertEquals("The primary key argument name must match across batch rows", e.getMessage());
    }
  }

  @Test
  public void batchInsertPkSeq() {
    db.dropSequenceQuietly("seq");
    new Schema().addTable("dbtest")
        .addColumn("pk").primaryKey().table()
        .addColumn("s").asString(10).schema()
        .addSequence("seq").schema().execute(db);

    db.toInsert("insert into dbtest (pk,s) values (?,?)")
        .argPkSeq("seq").argString("hi").batch()
        .argPkSeq("seq").argString("hello").batch()
        .argPkSeq("seq").argString("howdy").batch().insertBatch();

    assertEquals(3, db.toSelect("select count(*) from dbtest").queryIntegerOrZero());

    try {
      db.toInsert("insert into dbtest (pk,s) values (?,?)")
          .argPkSeq("seq").argString("hi").batch()
          // argPkLong in different position ==> error
          .argString("hello").argPkSeq("seq").batch().insertBatch();
      fail("Expecting an exception to be thrown");
    } catch (DatabaseException e) {
      assertEquals("The argPkSeq() calls must be in the same position across batch records", e.getMessage());
    }

    try {
      db.toInsert("insert into dbtest (pk,s) values (?,?)")
          // multiple pk calls ==> error
          .argPkSeq("seq").argPkSeq("seq").batch().insertBatch();
      fail("Expecting an exception to be thrown");
    } catch (DatabaseException e) {
      assertEquals("Only call one argPk*() method", e.getMessage());
    }
  }

  @Test
  public void batchInsertPkSeqNamed() {
    db.dropSequenceQuietly("seq");
    new Schema().addTable("dbtest")
        .addColumn("pk").primaryKey().table()
        .addColumn("s").asString(10).schema()
        .addSequence("seq").schema().execute(db);

    db.toInsert("insert into dbtest (pk,s) values (:pk,?)")
        .argPkSeq("pk", "seq").argString("hi").batch()
        .argPkSeq("pk", "seq").argString("hello").batch()
        .argPkSeq("pk", "seq").argString("howdy").batch().insertBatch();

    assertEquals(3, db.toSelect("select count(*) from dbtest").queryIntegerOrZero());

    db.toInsert("insert into dbtest (pk,s) values (:pk,?)")
        .batch().argPkSeq("pk", "seq").argString("hi").batch()
        .argString("hello").argPkSeq("pk", "seq").insertBatch();

    assertEquals(5, db.toSelect("select count(*) from dbtest").queryIntegerOrZero());

    try {
      db.toInsert("insert into dbtest (pk,s) values (:pk,?)")
          // multiple pk calls ==> error
          .argPkSeq("pk", "seq").argPkSeq("pk", "seq").batch().insertBatch();
      fail("Expecting an exception to be thrown");
    } catch (DatabaseException e) {
      assertEquals("Only call one argPk*() method", e.getMessage());
    }

    try {
      db.toInsert("insert into dbtest (pk,s) values (?,?)")
          .argPkSeq("pk", "seq").argString("howdy").batch()
          // different name for pk on second batch ==> error
          .argPkSeq("na", "seq").argString("hello").batch().insertBatch();
      fail("Expecting an exception to be thrown");
    } catch (DatabaseException e) {
      assertEquals("The primary key argument name must match across batch rows", e.getMessage());
    }
  }

  @Test
  public void bigClob() {
    new Schema().addTable("dbtest").addColumn("str_lob").asClob().schema().execute(db);

    StringBuilder buf = new StringBuilder();
    for (int i = 0; i < 40000; i++) {
      buf.append("0123456789");
    }
    final String longString = buf.toString();

    db.toInsert("insert into dbtest values (?)").argClobString(longString).insert(1);
    db.toSelect("select str_lob from dbtest").query(new RowsHandler<Void>() {
      @Override
      public Void process(Rows rs) throws Exception {
        assertTrue(rs.next());
        assertEquals(longString, rs.getClobStringOrNull(1));
        assertEquals(longString, rs.getClobStringOrNull("str_lob"));
        assertEquals(longString, readerToString(rs.getClobReaderOrNull(1)));
        return null;
      }
    });
    // Intentional slight variation here to test get()
    db.get().toSelect("select str_lob from dbtest").query(new RowsHandler<Void>() {
      @Override
      public Void process(Rows rs) throws Exception {
        assertTrue(rs.next());
        assertEquals(longString, readerToString(rs.getClobReaderOrNull("str_lob")));
        return null;
      }
    });
    db.toDelete("delete from dbtest").update(1);
    db.toInsert("insert into dbtest values (?)").argClobReader(new StringReader(longString)).insert(1);
    db.toSelect("select str_lob from dbtest").query(new RowsHandler<Void>() {
      @Override
      public Void process(Rows rs) throws Exception {
        assertTrue(rs.next());
        assertEquals(longString, rs.getClobStringOrNull(1));
        assertEquals(longString, rs.getClobStringOrNull("str_lob"));
        assertEquals(longString, readerToString(rs.getClobReaderOrNull(1)));
        return null;
      }
    });
    db.toSelect("select str_lob from dbtest").query(new RowsHandler<Void>() {
      @Override
      public Void process(Rows rs) throws Exception {
        assertTrue(rs.next());
        assertEquals(longString, readerToString(rs.getClobReaderOrNull("str_lob")));
        return null;
      }
    });
  }

  @Test
  public void bigBlob() {
    new Schema().addTable("dbtest").addColumn("bin_blob").asBlob().schema().execute(db);

    StringBuilder buf = new StringBuilder();
    for (int i = 0; i < 40000; i++) {
      buf.append("0123456789");
    }
    final byte[] bigBytes = buf.toString().getBytes();

    db.toInsert("insert into dbtest values (?)").argBlobBytes(bigBytes).insert(1);
    db.toSelect("select bin_blob from dbtest").query(new RowsHandler<Void>() {
      @Override
      public Void process(Rows rs) throws Exception {
        assertTrue(rs.next());
        assertArrayEquals(bigBytes, rs.getBlobBytesOrNull(1));
        assertArrayEquals(bigBytes, rs.getBlobBytesOrNull("bin_blob"));
        assertArrayEquals(bigBytes, inputStreamToString(rs.getBlobInputStreamOrNull(1)));
        return null;
      }
    });
    db.toSelect("select bin_blob from dbtest").query(new RowsHandler<Void>() {
      @Override
      public Void process(Rows rs) throws Exception {
        assertTrue(rs.next());
        assertArrayEquals(bigBytes, inputStreamToString(rs.getBlobInputStreamOrNull("bin_blob")));
        return null;
      }
    });
    db.toDelete("delete from dbtest").update(1);
    db.toInsert("insert into dbtest values (?)").argBlobStream(new ByteArrayInputStream(bigBytes)).insert(1);
    db.toSelect("select bin_blob from dbtest").query(new RowsHandler<Void>() {
      @Override
      public Void process(Rows rs) throws Exception {
        assertTrue(rs.next());
        assertArrayEquals(bigBytes, rs.getBlobBytesOrNull(1));
        assertArrayEquals(bigBytes, rs.getBlobBytesOrNull("bin_blob"));
        assertArrayEquals(bigBytes, inputStreamToString(rs.getBlobInputStreamOrNull(1)));
        return null;
      }
    });
    db.toSelect("select bin_blob from dbtest").query(new RowsHandler<Void>() {
      @Override
      public Void process(Rows rs) throws Exception {
        assertTrue(rs.next());
        assertArrayEquals(bigBytes, inputStreamToString(rs.getBlobInputStreamOrNull("bin_blob")));
        return null;
      }
    });
  }

  @Test
  public void argLocalDateTimeZones() {
    LocalDate januaryOne2000 = LocalDate.of(2000, Month.JANUARY, 1);

    // Verify we always get the same LocalDate regardless of time zone and DB across all drivers
    new Schema().addTable("dbtest").addColumn("i").asLocalDate().schema().execute(db);
    db.toInsert("insert into dbtest (i) values (?)").argLocalDate(januaryOne2000).insert(1);

    // Query without specifying a zone
    assertEquals(januaryOne2000,
        db.toSelect("select i from dbtest where i=?").argLocalDate(januaryOne2000).queryLocalDateOrNull());

    TimeZone defaultTZ = TimeZone.getDefault();

    try {
      String[] availableTZs = TimeZone.getAvailableIDs();
      for (String tz : availableTZs) {
        TimeZone.setDefault(TimeZone.getTimeZone(tz));
        LocalDate result =
                db.toSelect("select i from dbtest where i=?").argLocalDate(januaryOne2000).queryLocalDateOrNull();
        assertEquals(januaryOne2000, result);
      }
    } finally {
      TimeZone.setDefault(defaultTZ);
    }
  }

  @Test
  public void argLocalDateLeapYear() {
    new Schema().addTable("dbtest").addColumn("testdate").asLocalDate().schema().execute(db);

    // Start by adding Febriary 28 and March 1 of 1900.  This was not a leap year.
    LocalDate feb1900 = LocalDate.of(1900, Month.FEBRUARY, 28);
    db.toInsert("insert into dbtest (testdate) values (?)").argLocalDate(feb1900).insert(1);
    assertEquals(feb1900,
        db.toSelect("select testdate from dbtest where testdate=?").argLocalDate(feb1900).queryLocalDateOrNull());

    LocalDate mar1900 = LocalDate.of(1900, Month.MARCH, 1);
    db.toInsert("insert into dbtest (testdate) values (?)").argLocalDate(mar1900).insert(1);
    assertEquals(mar1900,
        db.toSelect("select testdate from dbtest where testdate=?").argLocalDate(mar1900).queryLocalDateOrNull());

    // Now try Feb 28, 29, and March 1 of 2000.  This was a leap year
    LocalDate feb2000 = LocalDate.of(2000, Month.FEBRUARY, 28);
    db.toInsert("insert into dbtest (testdate) values (?)").argLocalDate(feb2000).insert(1);
    assertEquals(feb2000,
        db.toSelect("select testdate from dbtest where testdate=?").argLocalDate(feb2000).queryLocalDateOrNull());

    LocalDate febLeap2000 = LocalDate.of(2000, Month.FEBRUARY, 29);
    db.toInsert("insert into dbtest (testdate) values (?)").argLocalDate(febLeap2000).insert(1);
    assertEquals(febLeap2000,
        db.toSelect("select testdate from dbtest where testdate=?").argLocalDate(febLeap2000).queryLocalDateOrNull());

    LocalDate mar2000 = LocalDate.of(2000, Month.MARCH, 1);
    db.toInsert("insert into dbtest (testdate) values (?)").argLocalDate(mar2000).insert(1);
    assertEquals(mar2000,
        db.toSelect("select testdate from dbtest where testdate=?").argLocalDate(mar2000).queryLocalDateOrNull());
  }
  
  @Test
  public void argIntegerMinMax() {
    new Schema().addTable("dbtest").addColumn("i").asInteger().schema().execute(db);

    db.toInsert("insert into dbtest (i) values (?)").argInteger(Integer.MIN_VALUE).insert(1);
    assertEquals(new Integer(Integer.MIN_VALUE),
        db.toSelect("select i from dbtest where i=?").argInteger(Integer.MIN_VALUE).queryIntegerOrNull());

    db.toInsert("insert into dbtest (i) values (?)").argInteger(Integer.MAX_VALUE).insert(1);
    assertEquals(new Integer(Integer.MAX_VALUE),
        db.toSelect("select i from dbtest where i=?").argInteger(Integer.MAX_VALUE).queryIntegerOrNull());
  }

  @Test
  public void argLongMinMax() {
    new Schema().addTable("dbtest").addColumn("i").asLong().schema().execute(db);

    db.toInsert("insert into dbtest (i) values (?)").argLong(Long.MIN_VALUE).insert(1);
    assertEquals(new Long(Long.MIN_VALUE),
        db.toSelect("select i from dbtest where i=?").argLong(Long.MIN_VALUE).queryLongOrNull());

    db.toInsert("insert into dbtest (i) values (?)").argLong(Long.MAX_VALUE).insert(1);
    assertEquals(new Long(Long.MAX_VALUE),
        db.toSelect("select i from dbtest where i=?").argLong(Long.MAX_VALUE).queryLongOrNull());
  }

  @Test
  public void argFloatMinMax() {
    new Schema().addTable("dbtest").addColumn("i").asFloat().schema().execute(db);

    db.toInsert("insert into dbtest (i) values (?)").argFloat(Float.MIN_VALUE).insert(1);
    assertEquals(new Float(Float.MIN_VALUE),
        db.toSelect("select i from dbtest where i=?").argFloat(Float.MIN_VALUE).queryFloatOrNull());

    db.toInsert("insert into dbtest (i) values (?)").argFloat(Float.MAX_VALUE).insert(1);
    assertEquals(new Float(Float.MAX_VALUE),
        db.toSelect("select i from dbtest where i=?").argFloat(Float.MAX_VALUE).queryFloatOrNull());
  }

  @Test
  public void argFloatNaN() {
    new Schema().addTable("dbtest").addColumn("i").asFloat().schema().execute(db);

    db.toInsert("insert into dbtest (i) values (?)").argFloat(Float.NaN).insert(1);
    assertEquals(new Float(Float.NaN),
        db.toSelect("select i from dbtest where i=?").argFloat(Float.NaN).queryFloatOrNull());
  }

  @Test
  public void argFloatInfinity() {
    new Schema().addTable("dbtest").addColumn("i").asFloat().schema().execute(db);

    db.toInsert("insert into dbtest (i) values (?)").argFloat(Float.NEGATIVE_INFINITY).insert(1);
    assertEquals(new Float(Float.NEGATIVE_INFINITY),
        db.toSelect("select i from dbtest where i=?").argFloat(Float.NEGATIVE_INFINITY).queryFloatOrNull());

    db.toInsert("insert into dbtest (i) values (?)").argFloat(Float.POSITIVE_INFINITY).insert(1);
    assertEquals(new Float(Float.POSITIVE_INFINITY),
        db.toSelect("select i from dbtest where i=?").argFloat(Float.POSITIVE_INFINITY).queryFloatOrNull());
  }

  @Test
  public void argFloatZero() {
    new Schema().addTable("dbtest").addColumn("i").asFloat().schema().execute(db);

    db.toInsert("insert into dbtest (i) values (?)").argFloat(0f).insert(1);
    assertEquals(new Float(0f),
        db.toSelect("select i from dbtest where i=?").argFloat(0f).queryFloatOrNull());
  }

  @Test
  public void argFloatNegativeZero() {
    new Schema().addTable("dbtest").addColumn("i").asFloat().schema().execute(db);

    db.toInsert("insert into dbtest (i) values (?)").argFloat(-0f).insert(1);
    assertEquals(new Float(-0f),
        db.toSelect("select i from dbtest where i=?").argFloat(-0f).queryFloatOrNull());
  }

  @Test
  public void argDoubleMinMax() {
    new Schema().addTable("dbtest").addColumn("i").asDouble().schema().execute(db);

    db.toInsert("insert into dbtest (i) values (?)").argDouble(Double.MIN_VALUE).insert(1);
    assertEquals(new Double(Double.MIN_VALUE),
        db.toSelect("select i from dbtest where i=?").argDouble(Double.MIN_VALUE).queryDoubleOrNull());

    db.toInsert("insert into dbtest (i) values (?)").argDouble(Double.MAX_VALUE).insert(1);
    assertEquals(new Double(Double.MAX_VALUE),
        db.toSelect("select i from dbtest where i=?").argDouble(Double.MAX_VALUE).queryDoubleOrNull());
  }

  @Test
  public void argDoubleNaN() {
    new Schema().addTable("dbtest").addColumn("i").asDouble().schema().execute(db);

    db.toInsert("insert into dbtest (i) values (?)").argDouble(Double.NaN).insert(1);
    assertEquals(new Double(Double.NaN),
        db.toSelect("select i from dbtest where i=?").argDouble(Double.NaN).queryDoubleOrNull());
  }

  @Test
  public void argDoubleInfinity() {
    new Schema().addTable("dbtest").addColumn("i").asDouble().schema().execute(db);

    db.toInsert("insert into dbtest (i) values (?)").argDouble(Double.NEGATIVE_INFINITY).insert(1);
    assertEquals(new Double(Double.NEGATIVE_INFINITY),
        db.toSelect("select i from dbtest where i=?").argDouble(Double.NEGATIVE_INFINITY).queryDoubleOrNull());

    db.toInsert("insert into dbtest (i) values (?)").argDouble(Double.POSITIVE_INFINITY).insert(1);
    assertEquals(new Double(Double.POSITIVE_INFINITY),
        db.toSelect("select i from dbtest where i=?").argDouble(Double.POSITIVE_INFINITY).queryDoubleOrNull());
  }

  @Test
  public void argDoubleZero() {
    new Schema().addTable("dbtest").addColumn("i").asDouble().schema().execute(db);

    db.toInsert("insert into dbtest (i) values (?)").argDouble(0d).insert(1);
    assertEquals(new Double(0d),
        db.toSelect("select i from dbtest where i=?").argDouble(0d).queryDoubleOrNull());
  }

  @Test
  public void argDoubleNegativeZero() {
    new Schema().addTable("dbtest").addColumn("i").asDouble().schema().execute(db);

    db.toInsert("insert into dbtest (i) values (?)").argDouble(-0d).insert(1);
    assertEquals(new Double(-0d),
        db.toSelect("select i from dbtest where i=?").argDouble(-0d).queryDoubleOrNull());
  }

  @Test
  public void argBigDecimal38Precision0() {
    new Schema().addTable("dbtest").addColumn("i").asBigDecimal(38, 0).schema().execute(db);

    BigDecimal value = new BigDecimal("99999999999999999999999999999999999999"); // 38 digits
    db.toInsert("insert into dbtest (i) values (?)").argBigDecimal(value).insert(1);
    assertEquals(value,
        db.toSelect("select i from dbtest where i=?").argBigDecimal(value).queryBigDecimalOrNull());
  }

  @Test
  public void argBigDecimal38Precision1() {
    new Schema().addTable("dbtest").addColumn("i").asBigDecimal(38, 1).schema().execute(db);

    BigDecimal value = new BigDecimal("9999999999999999999999999999999999999.9"); // 38 digits
    db.toInsert("insert into dbtest (i) values (?)").argBigDecimal(value).insert(1);
    assertEquals(value,
        db.toSelect("select i from dbtest where i=?").argBigDecimal(value).queryBigDecimalOrNull());
  }

  @Test
  public void argBigDecimal38Precision37() {
    new Schema().addTable("dbtest").addColumn("i").asBigDecimal(38, 37).schema().execute(db);

    BigDecimal value = new BigDecimal("9.9999999999999999999999999999999999999"); // 38 digits
    db.toInsert("insert into dbtest (i) values (?)").argBigDecimal(value).insert(1);
    assertEquals(value,
        db.toSelect("select i from dbtest where i=?").argBigDecimal(value).queryBigDecimalOrNull());
  }

  @Test
  public void argBigDecimal38Precision38() {
    new Schema().addTable("dbtest").addColumn("i").asBigDecimal(38, 38).schema().execute(db);

    BigDecimal value = new BigDecimal("0.99999999999999999999999999999999999999"); // 38 digits
    db.toInsert("insert into dbtest (i) values (?)").argBigDecimal(value).insert(1);
    System.out.println(db.toSelect("select i from dbtest").queryBigDecimalOrNull());
    assertEquals(value,
        db.toSelect("select i from dbtest where i=?").argBigDecimal(value).queryBigDecimalOrNull());
  }

  @Test
  public void dropTableQuietly() {
    db.dropTableQuietly("dbtest");
    new Schema().addTable("dbtest").addColumn("pk").primaryKey().schema().execute(db);
    db.dropTableQuietly("dbtest");
    // Verify the quietly part really kicks in, since the table might have existed above
    db.dropTableQuietly("dbtest");
    new Schema().addTable("dbtest").addColumn("pk").primaryKey().schema().execute(db);
  }

  @Test
  public void dropSequenceQuietly() {
    db.dropSequenceQuietly("dbtest_seq");
    // Verify the quietly part really kicks in, since the sequence might have existed above
    db.dropSequenceQuietly("dbtest_seq");
  }

  @Test
  public void insertReturningPkSeq() {
    db.dropSequenceQuietly("dbtest_seq");

    db.ddl("create table dbtest (pk numeric)").execute();
    db.ddl("create sequence dbtest_seq start with 1").execute();

    assertEquals(new Long(1L), db.toInsert("insert into dbtest (pk) values (:seq)")
        .argPkSeq(":seq", "dbtest_seq").insertReturningPkSeq("pk"));
    assertEquals(new Long(2L), db.toInsert("insert into dbtest (pk) values (:seq)")
        .argPkSeq(":seq", "dbtest_seq").insertReturningPkSeq("pk"));
  }

  @Test
  public void insertReturningAppDate() {
    db.dropSequenceQuietly("dbtest_seq");

    new Schema()
        .addTable("dbtest")
          .addColumn("pk").primaryKey().table()
          .addColumn("d").asDate().table().schema()
        .addSequence("dbtest_seq").schema()
        .execute(db);

    db.toInsert("insert into dbtest (pk, d) values (:seq, :d)")
        .argPkSeq(":seq", "dbtest_seq")
        .argDateNowPerApp(":d")
        .insertReturning("dbtest", "pk", new RowsHandler<Object>() {
          @Override
          public Object process(Rows rs) throws Exception {
            assertTrue(rs.next());
            assertEquals(new Long(1L), rs.getLongOrNull(1));
            assertThat(rs.getDateOrNull(2), equalTo(now));
            assertFalse(rs.next());
            return null;
          }
        }, "d");
    assertEquals(new Long(1L), db.toSelect("select count(*) from dbtest where d=?").argDate(now).queryLongOrNull());
  }

  @Test
  public void quickQueries() {
    new Schema()
        .addTable("dbtest")
        .addColumn("pk").primaryKey().table()
        .addColumn("d").asDate().table()
        .addColumn("d2").asDate().table()
        .addColumn("d3").asLocalDate().table()
        .addColumn("d4").asLocalDate().table()
        .addColumn("s").asString(5).table()
        .addColumn("s2").asString(5).table()
        .addColumn("i").asInteger().table().schema()
        .execute(db);

    db.toInsert("insert into dbtest (pk, d, d3, s) values (?,?,?,?)")
        .argLong(1L).argDateNowPerApp().argLocalDate(localDateNow).argString("foo").insert(1);

    assertEquals(new Long(1L), db.toSelect("select pk from dbtest").queryLongOrNull());
    assertNull(db.toSelect("select pk from dbtest where 1=0").queryLongOrNull());
    assertNull(db.toSelect("select i from dbtest").queryLongOrNull());
    assertEquals(1L, db.toSelect("select pk from dbtest").queryLongOrZero());
    assertEquals(0L, db.toSelect("select pk from dbtest where 1=0").queryLongOrZero());
    assertEquals(0L, db.toSelect("select i from dbtest").queryLongOrZero());
    assertTrue(db.toSelect("select pk from dbtest").queryLongs().get(0) == 1L);
    assertTrue(db.toSelect("select pk from dbtest where 1=0").queryLongs().isEmpty());
    assertTrue(db.toSelect("select i from dbtest").queryLongs().isEmpty());

    assertEquals(new Integer(1), db.toSelect("select pk from dbtest").queryIntegerOrNull());
    assertNull(db.toSelect("select pk from dbtest where 1=0").queryIntegerOrNull());
    assertNull(db.toSelect("select i from dbtest").queryIntegerOrNull());
    assertEquals(1, db.toSelect("select pk from dbtest").queryIntegerOrZero());
    assertEquals(0, db.toSelect("select pk from dbtest where 1=0").queryIntegerOrZero());
    assertEquals(0, db.toSelect("select i from dbtest").queryIntegerOrZero());
    assertTrue(db.toSelect("select pk from dbtest").queryIntegers().get(0) == 1L);
    assertTrue(db.toSelect("select pk from dbtest where 1=0").queryIntegers().isEmpty());
    assertTrue(db.toSelect("select i from dbtest").queryIntegers().isEmpty());

    assertEquals("foo", db.toSelect("select s from dbtest").queryStringOrNull());
    assertNull(db.toSelect("select s from dbtest where 1=0").queryStringOrNull());
    assertNull(db.toSelect("select s2 from dbtest").queryStringOrNull());
    assertEquals("foo", db.toSelect("select s from dbtest").queryStringOrEmpty());
    assertEquals("", db.toSelect("select s from dbtest where 1=0").queryStringOrEmpty());
    assertEquals("", db.toSelect("select s2 from dbtest").queryStringOrEmpty());
    assertTrue(db.toSelect("select s from dbtest").queryStrings().get(0).equals("foo"));
    assertTrue(db.toSelect("select s from dbtest where 1=0").queryStrings().isEmpty());
    assertTrue(db.toSelect("select s2 from dbtest").queryStrings().isEmpty());

    assertEquals(now, db.toSelect("select d from dbtest").queryDateOrNull());
    assertNull(db.toSelect("select d from dbtest where 1=0").queryDateOrNull());
    assertNull(db.toSelect("select d2 from dbtest").queryDateOrNull());
    assertTrue(db.toSelect("select d from dbtest").queryDates().get(0).equals(now));
    assertTrue(db.toSelect("select d from dbtest where 1=0").queryDates().isEmpty());
    assertTrue(db.toSelect("select d2 from dbtest").queryDates().isEmpty());

    assertEquals(localDateNow, db.toSelect("select d3 from dbtest").queryLocalDateOrNull());
    assertNull(db.toSelect("select d3 from dbtest where 1=0").queryLocalDateOrNull());
    assertTrue(db.toSelect("select d3 from dbtest").queryLocalDates().get(0).equals(localDateNow));
    assertEquals(new Long(1L),
        db.toSelect("select count(*) from dbtest where d3=?").argLocalDate(localDateNow).queryLongOrNull());

    assertNull(db.toSelect("select d4 from dbtest").queryLocalDateOrNull());
    assertNull(db.toSelect("select d4 from dbtest where 1=0").queryLocalDateOrNull());
    assertTrue(db.toSelect("select d4 from dbtest").queryLocalDates().isEmpty());
  }

  @Test
  public void rowHandlerQueries() {
    new Schema()
        .addTable("dbtest")
        .addColumn("pk").primaryKey().schema()
        .execute(db);

    db.toInsert("insert into dbtest (pk) values (?)").argLong(1L).insert(1);
    db.toInsert("insert into dbtest (pk) values (?)").argLong(2L).insert(1);

    RowHandler<Long> rowHandler = new RowHandler<Long>() {
      @Override
      public Long process(Row r) throws Exception {
        return r.getLongOrNull();
      }
    };

    List<Long> many = db.toSelect("select pk from dbtest").queryMany(rowHandler);
    assertEquals(2, many.size());

    assertEquals(new Long(1), db.toSelect("select pk from dbtest where pk=1").queryOneOrNull(rowHandler));
    assertNull(db.toSelect("select pk from dbtest where pk=9").queryOneOrNull(rowHandler));
    try {
      db.toSelect("select pk from dbtest").queryOneOrNull(rowHandler);
      fail("Should have thrown an exception");
    } catch (ConstraintViolationException e) {
      assertEquals("Expected exactly one row to be returned but found multiple", e.getCause().getMessage());
    }
    try {
      db.toSelect("select pk from dbtest where pk=9").queryOneOrThrow(rowHandler);
      fail("Should have thrown an exception");
    } catch (ConstraintViolationException e) {
      assertEquals("Expected exactly one row to be returned but found none", e.getMessage());
    }

    assertEquals(new Long(1), db.toSelect("select pk from dbtest where pk=1").queryFirstOrNull(rowHandler));
    assertEquals(new Long(1), db.toSelect("select pk from dbtest order by 1").queryFirstOrNull(rowHandler));
    assertNull(db.toSelect("select pk from dbtest where pk=9").queryFirstOrNull(rowHandler));
    try {
      db.toSelect("select pk from dbtest where pk=9").queryFirstOrThrow(rowHandler);
      fail("Should have thrown an exception");
    } catch (ConstraintViolationException e) {
      assertEquals("Expected one or more rows to be returned but found none", e.getMessage());
    }
  }

  @Test
  public void nextSequenceValue() {
    db.dropSequenceQuietly("dbtest_seq");

    new Schema()
        .addSequence("dbtest_seq").schema()
        .execute(db);

    assertEquals(new Long(1L), db.nextSequenceValue("dbtest_seq"));
  }

  @Test
  public void insertReturningDbDate() {
    db.dropSequenceQuietly("dbtest_seq");

    new Schema()
        .addTable("dbtest")
          .addColumn("pk").primaryKey().table()
          .addColumn("d").asDate().table().schema()
        .addSequence("dbtest_seq").schema()
        .execute(db);

    Date dbNow = db.toInsert("insert into dbtest (pk, d) values (:seq, :d)")
        .argPkSeq(":seq", "dbtest_seq")
        .argDateNowPerDb(":d")
        .insertReturning("dbtest", "pk", new RowsHandler<Date>() {
          @Override
          public Date process(Rows rs) throws Exception {
            assertTrue(rs.next());
            assertEquals(new Long(1L), rs.getLongOrNull(1));
            Date dbDate = rs.getDateOrNull(2);
            assertFalse(rs.next());
            return dbDate;
          }
        }, "d");
//    System.err.println("***** d: " + db.select("select d from dbtest").queryString());
//    System.err.println("***** n: " + dbNow.getTime());
    assertEquals(new Long(1L), db.toSelect("select count(*) from dbtest where d=?").argDate(dbNow).queryLongOrNull());
  }

  @Test
  public void daylightSavings() {
    LocalDate lastStdDateSpring = LocalDate.of(2019, Month.MARCH, 9);
    LocalDate firstDSTDateSpring = LocalDate.of(2019, Month.MARCH, 10);

    // Verify that the original LocalDate matches the driver SQL LocalDate generated.
    StatementAdaptor adaptor = new StatementAdaptor(new OptionsDefault(db.flavor()));
    assertEquals(lastStdDateSpring.toString(), adaptor.nullLocalDate(lastStdDateSpring).toString());
    assertEquals(firstDSTDateSpring.toString(), adaptor.nullLocalDate(firstDSTDateSpring).toString());
  }

  @Test
  public void insertLocalDate() {
    // Date without time
    new Schema()
            .addTable("dbtest")
            .addColumn("d").asLocalDate().table().schema()
            .execute(db);

    LocalDate dateOfBirth = LocalDate.of(1951, Month.AUGUST, 9);
    db.toInsert("insert into dbtest (d) values (?)")
            .argLocalDate(dateOfBirth)
            .insert(1);

    LocalDate testDate = db.toSelect("select d from dbtest").queryLocalDateOrNull();
    assertEquals(dateOfBirth, testDate);
  }


  @Test
  public void localDateRoundTrip() {
    new Schema()
            .addTable("dbtest")
            .addColumn("d1").asLocalDate().table()
            .addColumn("d2").asLocalDate().table().schema()
            .execute(db);

    // Store current time as per the database
    db.toInsert("insert into dbtest (d1) values (?)")
            .argLocalDate(localDateNow)
            .insert(1);

    // Now pull it out, put it back in, and verify it matches in the database
    LocalDate queryRsDate = db.toSelect("select d1 from dbtest").queryLocalDateOrNull();

    db.toUpdate("update dbtest set d2=?")
            .argLocalDate(queryRsDate)
            .update(1);

    assertEquals(new Long(1L), db.toSelect("select count(*) from dbtest where d1=d2").queryLongOrNull());
  }

  /**
   * Enable retrying failed tests if they have the @Retry annotation.
   */
  @Rule
  public Retryable retry = new Retryable();


  /**
   * Make sure database times are inserted with at least millisecond precision.
   * This test is non-deterministic since it is checking the timestamp provided
   * by the database, so we use a retry mechanism to give it three attempts.
   */
  @Test @Retry
  public void dateMillis() {
    new Schema()
        .addTable("dbtest")
        .addColumn("d").asDate().table().schema()
        .execute(db);

    db.toInsert("insert into dbtest (d) values (?)")
        .argDateNowPerDb()
        .insert(1);

    Date dbNow = db.toSelect("select d from dbtest").queryDateOrNull();
    assertTrue("Timestamp had zero in the least significant digit", dbNow != null && dbNow.getTime() % 10 != 0);
  }

  @Test
  public void dateRoundTrip() {
    new Schema()
        .addTable("dbtest")
        .addColumn("d1").asDate().table()
        .addColumn("d2").asDate().table().schema()
        .execute(db);

    // Store current time as per the database
    db.toInsert("insert into dbtest (d1) values (?)")
        .argDateNowPerDb()
        .insert(1);

    // Now pull it out, put it back in, and verify it matches in the database
    Date dbNow = db.toSelect("select d1 from dbtest").queryDateOrNull();

    db.toUpdate("update dbtest set d2=?")
        .argDate(dbNow)
        .update(1);

//    System.err.println("***** d1: " + db.select("select to_char(d1) from dbtest").queryStringOrNull());
//    System.err.println("***** d2: " + db.select("select to_char(d2) from dbtest").queryStringOrNull());

    assertEquals(new Long(1L), db.toSelect("select count(*) from dbtest where d1=d2").queryLongOrNull());
  }

  @Test
  public void dateRoundTripTimezones() {
    new Schema()
        .addTable("dbtest")
        .addColumn("d").asDate().table().schema()
        .execute(db);

    Date date = new Date(166656789L);

    TimeZone defaultTZ = TimeZone.getDefault();

    try {
      TimeZone.setDefault(TimeZone.getTimeZone("GMT-4:00"));

      db.toInsert("insert into dbtest (d) values (?)").argDate(date).insert(1);
      assertEquals(date, db.toSelect("select d from dbtest").queryDateOrNull());
      assertEquals("1970-01-02 18:17:36.789000-0400", new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS000Z").format(
          db.toSelect("select d from dbtest").queryDateOrNull()));
      db.toDelete("delete from dbtest where d=?").argDate(date).update(1);

      TimeZone.setDefault(TimeZone.getTimeZone("GMT+4:00"));

      db.toInsert("insert into dbtest (d) values (?)").argDate(date).insert(1);
      assertEquals(date, db.toSelect("select d from dbtest").queryDateOrNull());
      assertEquals("1970-01-03 02:17:36.789000+0400", new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS000Z").format(
          db.toSelect("select d from dbtest").queryDateOrNull()));
      db.toDelete("delete from dbtest where d=?").argDate(date).update(1);
    } finally {
      TimeZone.setDefault(defaultTZ);
    }
  }

  /**
   * Verify the appropriate database flavor can correctly convert a {@code Date}
   * into a SQL function representing a conversion from string to timestamp. This
   * function is used to write debug SQL to the log in a way that could be manually
   * executed if desired.
   */
  @Test
  public void stringDateFunctions() {
    Date date = new Date(166656789L);
    System.out.println("Date: " + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS000Z").format(date));

    TimeZone defaultTZ = TimeZone.getDefault();

    try {
      TimeZone.setDefault(TimeZone.getTimeZone("GMT-4:00"));

      new Schema()
          .addTable("dbtest")
          .addColumn("d").asDate().schema().execute(db);

      db.toInsert("insert into dbtest (d) values ("
          + db.flavor().dateAsSqlFunction(date, db.options().calendarForTimestamps()).replace(":", "::") + ")")
          .insert(1);

      assertEquals("1970-01-02 18:17:36.789000-0400", new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS000Z").format(
          db.toSelect("select d from dbtest").queryDateOrNull()));

      // Now do some client operations in a different time zone
      TimeZone.setDefault(TimeZone.getTimeZone("GMT+4:00"));

      // Verify regular arg maps date the same way even though our TimeZone is now different
      db.toDelete("delete from dbtest where d=?").argDate(date).update(1);

      db.toInsert("insert into dbtest (d) values ("
          + db.flavor().dateAsSqlFunction(date, db.options().calendarForTimestamps()).replace(":", "::") + ")")
          .insert(1);

      assertEquals("1970-01-03 02:17:36.789000+0400", new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS000Z").format(
          db.toSelect("select d from dbtest").queryDateOrNull()));

      // Verify the function maps correctly for equals operations as well
      db.toDelete("delete from dbtest where d=" + db.flavor().dateAsSqlFunction(date,
          db.options().calendarForTimestamps()).replace(":", "::")).update(1);
    } finally {
      TimeZone.setDefault(defaultTZ);
    }
  }

  @Test
  public void mixPositionalAndNamedParameters() {
    new Schema()
        .addTable("dbtest")
        .addColumn("pk").primaryKey().table()
        .addColumn("d").asDate().table()
        .addColumn("a").asInteger().table().schema()
        .execute(db);

    db.toSelect("select pk as \"time:: now??\" from dbtest where a=? and d=:now")
        .argInteger(1).argDateNowPerDb("now").query(new RowsHandler<Object>() {
      @Override
      public Object process(Rows rs) throws Exception {
        assertFalse(rs.next());
        return null;
      }
    });
  }

  public String readerToString(Reader reader) throws IOException {
    char[] buffer = new char[1024];
    StringBuilder out = new StringBuilder();
    int byteCount;
    while ((byteCount = reader.read(buffer, 0, buffer.length)) >= 0) {
      out.append(buffer, 0, byteCount);
    }
    return out.toString();
  }

  public byte[] inputStreamToString(InputStream inputStream) throws IOException {
    byte[] buffer = new byte[1024];
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    int byteCount;
    while ((byteCount = inputStream.read(buffer, 0, buffer.length)) >= 0) {
      out.write(buffer, 0, byteCount);
    }
    return out.toByteArray();
  }
}
