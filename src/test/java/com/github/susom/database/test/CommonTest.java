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
import java.util.Date;
import java.util.List;

import org.apache.log4j.xml.DOMConfigurator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.github.susom.database.Database;
import com.github.susom.database.DatabaseProvider;
import com.github.susom.database.OptionsOverride;
import com.github.susom.database.Rows;
import com.github.susom.database.RowsHandler;
import com.github.susom.database.Schema;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.*;

/**
 * Exercise Database functionality with a real databases.
 *
 * @author garricko
 */
public abstract class CommonTest {
  static {
    // Initialize logging
    String log4jConfig = new File("log4j.xml").getAbsolutePath();
    DOMConfigurator.configure(log4jConfig);
    org.apache.log4j.Logger log = org.apache.log4j.Logger.getLogger(CommonTest.class);
    log.info("Initialized log4j using file: " + log4jConfig);
  }

  protected DatabaseProvider dbp;
  protected Database db;
  protected Date now;

  @Before
  public void setupJdbc() throws Exception {
    dbp = createDatabaseProvider(new OptionsOverride() {
      @Override
      public Date currentDate() {
        return now;
      }
    });
    db = dbp.get();
  }

  protected abstract DatabaseProvider createDatabaseProvider(OptionsOverride options) throws Exception;

  @After
  public void closeJdbc() throws Exception {
    if (dbp != null) {
      dbp.commitAndClose();
    }
  }

  @Test
  public void selectNewTable() {
    db.dropTableQuietly("dbtest");

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
          .addColumn("date_millis").asDate().table().schema().execute(db);

    now = new Date();
    BigDecimal bigDecimal = new BigDecimal("5.3");
    db.insert("insert into dbtest values (?,?,?,?,?,?,?,?,?,?)").argInteger(1).argLong(2L).argFloat(3.2f).argDouble(4.2)
        .argBigDecimal(bigDecimal).argString("Hello").argString("T").argClobString("World")
        .argBlobBytes("More".getBytes()).argDate(now).insert(1);

    db.select("select nbr_integer, nbr_long, nbr_float, nbr_double, nbr_big_decimal, str_varchar, str_fixed, str_lob, "
        + "bin_blob, date_millis from dbtest").query(new RowsHandler<Void>() {
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
        return null;
      }
    });
    db.select("select str_lob, bin_blob from dbtest").query(new RowsHandler<Void>() {
      @Override
      public Void process(Rows rs) throws Exception {
        assertTrue(rs.next());
        assertEquals("World", readerToString(rs.getClobReaderOrNull(1)));
        assertArrayEquals("More".getBytes(), inputStreamToString(rs.getBlobInputStreamOrNull(2)));
        return null;
      }
    });
    db.select("select str_lob, bin_blob from dbtest").query(new RowsHandler<Void>() {
      @Override
      public Void process(Rows rs) throws Exception {
        assertTrue(rs.next());
        assertEquals("World", readerToString(rs.getClobReaderOrNull("str_lob")));
        assertArrayEquals("More".getBytes(), inputStreamToString(rs.getBlobInputStreamOrNull("bin_blob")));
        return null;
      }
    });

    assertEquals(new Long(1), db.select("select count(*) from dbtest where nbr_integer=:i and nbr_long=:l and "
        + "abs(nbr_float-:f)<0.01 and abs(nbr_double-:d)<0.01 and nbr_big_decimal=:bd and str_varchar=:s "
        + "and str_fixed=:sf and date_millis=:date").argInteger("i", 1).argLong("l", 2L).argFloat("f", 3.2f)
        .argDouble("d", 4.2).argBigDecimal("bd", bigDecimal).argString("s", "Hello").argString("sf", "T")
        .argDate("date", now).queryLongOrNull());
    List<Long> result = db.select("select count(*) from dbtest where nbr_integer=:i and nbr_long=:l and "
        + "abs(nbr_float-:f)<0.01 and abs(nbr_double-:d)<0.01 and nbr_big_decimal=:bd and str_varchar=:s "
        + "and str_fixed=:sf and date_millis=:date").argInteger("i", 1).argLong("l", 2L).argFloat("f", 3.2f)
        .argDouble("d", 4.2).argBigDecimal("bd", bigDecimal).argString("s", "Hello").argString("sf", "T")
        .argDate("date", now).queryLongs();
    assertEquals(1, result.size());
    assertEquals(new Long(1), result.get(0));
  }

  @Test
  public void updatePositionalArgs() {
    db.dropTableQuietly("dbtest");

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
        .addColumn("date_millis").asDate().table().schema().execute(db);

    now = new Date();
    BigDecimal bigDecimal = new BigDecimal("5.3");
    assertEquals(1, db.insert("insert into dbtest values (?,?,?,?,?,?,?,?,?,?,?)")
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
        .argDate(now).insert());

    db.update("update dbtest set nbr_integer=?, nbr_long=?, nbr_float=?, nbr_double=?, nbr_big_decimal=?, "
        + "str_varchar=?, str_fixed=?, str_lob=?, bin_blob=?, date_millis=?").argInteger(null).argLong(null)
        .argFloat(null).argDouble(null).argBigDecimal(null).argString(null).argString(null).argClobString(null)
        .argBlobBytes(null).argDate(null).update(1);
    db.select("select nbr_integer, nbr_long, nbr_float, nbr_double, nbr_big_decimal, str_varchar, str_fixed, str_lob, "
        + "bin_blob, date_millis from dbtest").query(new RowsHandler<Void>() {
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
        return null;
      }
    });
    assertEquals(1, db.update("update dbtest set nbr_integer=?, nbr_long=?, nbr_float=?, nbr_double=?, "
        + "nbr_big_decimal=?, str_varchar=?, str_fixed=?, str_lob=?, bin_blob=?, date_millis=?").argInteger(1)
        .argLong(2L).argFloat(3.2f).argDouble(4.2).argBigDecimal(bigDecimal).argString("Hello").argString("T")
        .argClobString("World").argBlobBytes("More".getBytes()).argDate(now).update());
    db.select("select nbr_integer, nbr_long, nbr_float, nbr_double, nbr_big_decimal, str_varchar, str_fixed, str_lob, "
        + "bin_blob, date_millis from dbtest").query(new RowsHandler<Void>() {
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
        return null;
      }
    });

    db.update("update dbtest set str_lob=?, bin_blob=?").argClobReader(null).argBlobStream(null).update(1);
    db.select("select str_lob, bin_blob from dbtest").query(new RowsHandler<Void>() {
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
    db.update("update dbtest set str_lob=?, bin_blob=?").argClobReader(new StringReader("World"))
        .argBlobStream(new ByteArrayInputStream("More".getBytes())).update(1);
    db.select("select str_lob, bin_blob from dbtest").query(new RowsHandler<Void>() {
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
    db.dropTableQuietly("dbtest");

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
        .addColumn("date_millis").asDate().table().schema().execute(db);

    now = new Date();
    BigDecimal bigDecimal = new BigDecimal("5.3");
    db.insert("insert into dbtest values (:pk,:a,:b,:c,:d,:e,:f,:sf,:g,:h,:i)").argLong(":pk", 1L).argInteger(":a", 1)
        .argLong(":b", 2L).argFloat(":c", 3.2f).argDouble(":d", 4.2).argBigDecimal(":e", bigDecimal)
        .argString(":f", "Hello").argString(":sf", "T")
        .argClobString(":g", "World").argBlobBytes(":h", "More".getBytes()).argDate(":i", now).insert(1);
    db.update("update dbtest set nbr_integer=:a, nbr_long=:b, nbr_float=:c, nbr_double=:d, nbr_big_decimal=:e, "
        + "str_varchar=:f, str_fixed=:sf, str_lob=:g, bin_blob=:h, date_millis=:i").argInteger(":a", null)
        .argLong(":b", null).argFloat(":c", null).argDouble(":d", null).argBigDecimal(":e", null)
        .argString(":f", null).argString(":sf", null)
        .argClobString(":g", null).argBlobBytes(":h", null).argDate(":i", null).update(1);
    db.select("select nbr_integer, nbr_long, nbr_float, nbr_double, nbr_big_decimal, str_varchar, str_fixed, str_lob, "
        + "bin_blob, date_millis from dbtest").query(new RowsHandler<Void>() {
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
        return null;
      }
    });
    db.update("update dbtest set nbr_integer=:a, nbr_long=:b, nbr_float=:c, nbr_double=:d, nbr_big_decimal=:e, "
        + "str_varchar=:f, str_fixed=:sf, str_lob=:g, bin_blob=:h, date_millis=:i").argInteger(":a", 1)
        .argLong(":b", 2L).argFloat(":c", 3.2f).argDouble(":d", 4.2).argBigDecimal(":e", bigDecimal)
        .argString(":f", "Hello").argString(":sf", "T")
        .argClobString(":g", "World").argBlobBytes(":h", "More".getBytes()).argDate(":i", now).update(1);
    db.select("select nbr_integer, nbr_long, nbr_float, nbr_double, nbr_big_decimal, str_varchar, str_fixed, str_lob, "
        + "bin_blob, date_millis from dbtest").query(new RowsHandler<Void>() {
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
        return null;
      }
    });

    db.update("update dbtest set str_lob=:a, bin_blob=:b").argClobReader(":a", null).argBlobStream(":b", null).update(1);
    db.select("select str_lob, bin_blob from dbtest").query(new RowsHandler<Void>() {
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
    db.update("update dbtest set str_lob=:a, bin_blob=:b").argClobReader(":a", new StringReader("World"))
        .argBlobStream(":b", new ByteArrayInputStream("More".getBytes())).update(1);
    db.select("select str_lob, bin_blob from dbtest").query(new RowsHandler<Void>() {
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
    db.dropTableQuietly("dbtest");

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
        .addColumn("date_millis").asDate().table().schema().execute(db);

    db.insert("insert into dbtest values (?,?,?,?,?,?,?,?,?,?,?)").argLong(1L).argInteger(null).argLong(null)
        .argFloat(null).argDouble(null).argBigDecimal(null).argString(null).argString(null).argClobString(null)
        .argBlobBytes(null).argDate(null).insert(1);
    db.select("select nbr_integer, nbr_long, nbr_float, nbr_double, nbr_big_decimal, str_varchar, str_fixed, str_lob, "
        + "bin_blob, date_millis from dbtest").query(new RowsHandler<Void>() {
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
        return null;
      }
    });
    db.select("select str_lob, bin_blob from dbtest").query(new RowsHandler<Void>() {
      @Override
      public Void process(Rows rs) throws Exception {
        assertTrue(rs.next());
        assertNull(rs.getClobReaderOrNull(1));
        assertNull(rs.getBlobInputStreamOrNull(2));
        return null;
      }
    });
    db.select("select str_lob, bin_blob from dbtest").query(new RowsHandler<Void>() {
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
  public void metadataColumnNames() {
    db.dropTableQuietly("dbtest");

    new Schema().addTable("dbtest").addColumn("pk").primaryKey().schema().execute(db);

    db.select("select Pk, Pk as Foo, Pk as \"Foo\" from dbtest")
        .query(new RowsHandler<Object>() {
          @Override
          public Object process(Rows rs) throws Exception {
            assertArrayEquals(new String[] { "PK", "FOO", "Foo" }, rs.getColumnNames());
            return null;
          }
        });
  }

  @Test
  public void bigClob() {
    db.dropTableQuietly("dbtest");

    new Schema().addTable("dbtest").addColumn("str_lob").asClob().schema().execute(db);

    StringBuilder buf = new StringBuilder();
    for (int i = 0; i < 40000; i++) {
      buf.append("0123456789");
    }
    final String longString = buf.toString();

    db.insert("insert into dbtest values (?)").argClobString(longString).insert(1);
    db.select("select str_lob from dbtest").query(new RowsHandler<Void>() {
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
    db.get().select("select str_lob from dbtest").query(new RowsHandler<Void>() {
      @Override
      public Void process(Rows rs) throws Exception {
        assertTrue(rs.next());
        assertEquals(longString, readerToString(rs.getClobReaderOrNull("str_lob")));
        return null;
      }
    });
    db.delete("delete from dbtest").update(1);
    db.insert("insert into dbtest values (?)").argClobReader(new StringReader(longString)).insert(1);
    db.select("select str_lob from dbtest").query(new RowsHandler<Void>() {
      @Override
      public Void process(Rows rs) throws Exception {
        assertTrue(rs.next());
        assertEquals(longString, rs.getClobStringOrNull(1));
        assertEquals(longString, rs.getClobStringOrNull("str_lob"));
        assertEquals(longString, readerToString(rs.getClobReaderOrNull(1)));
        return null;
      }
    });
    db.select("select str_lob from dbtest").query(new RowsHandler<Void>() {
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
    db.dropTableQuietly("dbtest");

    new Schema().addTable("dbtest").addColumn("bin_blob").asBlob().schema().execute(db);

    StringBuilder buf = new StringBuilder();
    for (int i = 0; i < 40000; i++) {
      buf.append("0123456789");
    }
    final byte[] bigBytes = buf.toString().getBytes();

    db.insert("insert into dbtest values (?)").argBlobBytes(bigBytes).insert(1);
    db.select("select bin_blob from dbtest").query(new RowsHandler<Void>() {
      @Override
      public Void process(Rows rs) throws Exception {
        assertTrue(rs.next());
        assertArrayEquals(bigBytes, rs.getBlobBytesOrNull(1));
        assertArrayEquals(bigBytes, rs.getBlobBytesOrNull("bin_blob"));
        assertArrayEquals(bigBytes, inputStreamToString(rs.getBlobInputStreamOrNull(1)));
        return null;
      }
    });
    db.select("select bin_blob from dbtest").query(new RowsHandler<Void>() {
      @Override
      public Void process(Rows rs) throws Exception {
        assertTrue(rs.next());
        assertArrayEquals(bigBytes, inputStreamToString(rs.getBlobInputStreamOrNull("bin_blob")));
        return null;
      }
    });
    db.delete("delete from dbtest").update(1);
    db.insert("insert into dbtest values (?)").argBlobStream(new ByteArrayInputStream(bigBytes)).insert(1);
    db.select("select bin_blob from dbtest").query(new RowsHandler<Void>() {
      @Override
      public Void process(Rows rs) throws Exception {
        assertTrue(rs.next());
        assertArrayEquals(bigBytes, rs.getBlobBytesOrNull(1));
        assertArrayEquals(bigBytes, rs.getBlobBytesOrNull("bin_blob"));
        assertArrayEquals(bigBytes, inputStreamToString(rs.getBlobInputStreamOrNull(1)));
        return null;
      }
    });
    db.select("select bin_blob from dbtest").query(new RowsHandler<Void>() {
      @Override
      public Void process(Rows rs) throws Exception {
        assertTrue(rs.next());
        assertArrayEquals(bigBytes, inputStreamToString(rs.getBlobInputStreamOrNull("bin_blob")));
        return null;
      }
    });
  }

  @Test
  public void dropTableQuietly() {
    db.dropTableQuietly("dbtest");
    // Verify the quietly part really kicks in, since the table might have existed above
    db.dropTableQuietly("dbtest");
  }

  @Test
  public void dropSequenceQuietly() {
    db.dropSequenceQuietly("dbtest_seq");
    // Verify the quietly part really kicks in, since the sequence might have existed above
    db.dropSequenceQuietly("dbtest_seq");
  }

  @Test
  public void insertReturningPkSeq() {
    db.dropTableQuietly("dbtest");
    db.dropSequenceQuietly("dbtest_seq");

    db.ddl("create table dbtest (pk numeric)").execute();
    db.ddl("create sequence dbtest_seq start with 1").execute();

    assertEquals(new Long(1L), db.insert("insert into dbtest (pk) values (:seq)")
        .argPkSeq(":seq", "dbtest_seq").insertReturningPkSeq("pk"));
    assertEquals(new Long(2L), db.insert("insert into dbtest (pk) values (:seq)")
        .argPkSeq(":seq", "dbtest_seq").insertReturningPkSeq("pk"));
  }

  @Test
  public void insertReturningAppDate() {
    db.dropTableQuietly("dbtest");
    db.dropSequenceQuietly("dbtest_seq");

    new Schema()
        .addTable("dbtest")
          .addColumn("pk").primaryKey().table()
          .addColumn("d").asDate().table().schema()
        .addSequence("dbtest_seq").schema()
        .execute(db);

    now = new Date();
    db.insert("insert into dbtest (pk, d) values (:seq, :d)")
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
    assertEquals(new Long(1L), db.select("select count(*) from dbtest where d=?").argDate(now).queryLongOrNull());
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
    db.dropTableQuietly("dbtest");
    db.dropSequenceQuietly("dbtest_seq");

    new Schema()
        .addTable("dbtest")
          .addColumn("pk").primaryKey().table()
          .addColumn("d").asDate().table().schema()
        .addSequence("dbtest_seq").schema()
        .execute(db);

    Date dbNow = db.insert("insert into dbtest (pk, d) values (:seq, :d)")
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
    assertEquals(new Long(1L), db.select("select count(*) from dbtest where d=?").argDate(dbNow).queryLongOrNull());
  }

  /**
   * Make sure database times are inserted will at least millisecond precision.
   */
  @Test
  public void dbDateMillis() {
    db.dropTableQuietly("dbtest");

    new Schema()
        .addTable("dbtest")
        .addColumn("d").asDate().table().schema()
        .execute(db);

    // Insert dates repeatedly until we get one with non-zero in the last millisecond digit
    int attempts = 0;
    int maxAttempts = 50;
    do {
      db.insert("insert into dbtest (d) values (?)")
          .argDateNowPerDb()
          .insert(1);

      Date dbNow = db.select("select d from dbtest").queryDateOrNull();

//      System.err.println("***** d: " + db.select("select to_char(d) from dbtest").queryStringOrNull());

      if (dbNow != null && dbNow.getTime() % 10 != 0) {
        break;
      }
    } while (++attempts < maxAttempts);

    assertTrue(attempts < maxAttempts);
  }

  @Test
  public void dbDateRoundTrip() {
    db.dropTableQuietly("dbtest");

    new Schema()
        .addTable("dbtest")
        .addColumn("d1").asDate().table()
        .addColumn("d2").asDate().table().schema()
        .execute(db);

    // Store current time as per the database
    db.insert("insert into dbtest (d1) values (?)")
        .argDateNowPerDb()
        .insert(1);

    // Now pull it out, put it back in, and verify it matches in the database
    Date dbNow = db.select("select d1 from dbtest").queryDateOrNull();

    db.update("update dbtest set d2=?")
        .argDate(dbNow)
        .update();

//    System.err.println("***** d1: " + db.select("select to_char(d1) from dbtest").queryStringOrNull());
//    System.err.println("***** d2: " + db.select("select to_char(d2) from dbtest").queryStringOrNull());

    assertEquals(new Long(1L), db.select("select count(*) from dbtest where d1=d2").queryLongOrNull());
  }

  @Test
  public void mixPositionalAndNamedParameters() {
    db.dropTableQuietly("dbtest");

    new Schema()
        .addTable("dbtest")
        .addColumn("pk").primaryKey().table()
        .addColumn("d").asDate().table()
        .addColumn("a").asInteger().table().schema()
        .execute(db);

    db.select("select pk as \"time:: now??\" from dbtest where a=? and d=:now")
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
