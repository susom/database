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
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.logging.ConsoleHandler;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.github.susom.database.Database;
import com.github.susom.database.DatabaseImpl;
import com.github.susom.database.Flavor;
import com.github.susom.database.OptionsDefault;
import com.github.susom.database.Rows;
import com.github.susom.database.RowsHandler;

import static org.junit.Assert.*;

/**
 * Exercise Database functionality with a real Oracle database.
 *
 * @author garricko
 */
public class PostgreSqlTest {
  static {
    // Turn on logging so we can inspect queries and errors
    Logger logger = Logger.getLogger("edu.stanford");
    logger.setLevel(Level.FINEST);

    ConsoleHandler handler = new ConsoleHandler();
    handler.setLevel(Level.FINEST);
    logger.addHandler(handler);
  }

  protected Connection c;
  protected Database db;

  @Before
  public void setupJdbc() throws Exception {
    c = createConnection();
    db = new DatabaseImpl(c, new OptionsDefault(Flavor.postgresql));
  }

  protected Connection createConnection() throws Exception {
    Properties properties = new Properties();
    try {
      properties.load(new FileReader(System.getProperty("build.properties", "../build.properties")));
    } catch (Exception e) {
      // Don't care, fallback to system properties
    }

    Class.forName("org.postgresql.Driver");

    return DriverManager.getConnection(
        System.getProperty("postgres.database.url", properties.getProperty("postgres.database.url")),
        System.getProperty("postgres.database.user", properties.getProperty("postgres.database.user")),
        System.getProperty("postgres.database.password", properties.getProperty("postgres.database.password")));
  }

  @After
  public void closeJdbc() throws Exception {
    if (c != null) {
      c.close();
    }
  }

  @Test
  public void selectNewTable() {
    db.ddl("drop table dbtest").executeQuietly();
    db.ddl("create table dbtest (nbr_integer numeric(8), nbr_long numeric(10), nbr_float numeric(9,3), "
        + "nbr_double numeric(19,9), nbr_big_decimal numeric(25,15), str_varchar varchar(80), str_lob text, "
        + "bin_blob bytea, date_millis timestamp)").execute();
    final Date currentDate = new Date();
    BigDecimal bigDecimal = new BigDecimal("5.3");
    db.insert("insert into dbtest values (?,?,?,?,?,?,?,?,?)").argInteger(1).argLong(2L).argFloat(3.2f).argDouble(4.2)
        .argBigDecimal(bigDecimal).argString("Hello").argClobString("World").argBlobBytes("More".getBytes())
        .argDate(currentDate).insert(1);
    db.select("select nbr_integer, nbr_long, nbr_float, nbr_double, nbr_big_decimal, str_varchar, str_lob, "
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
        assertEquals("World", rs.getClobStringOrNull(7));
        assertEquals("World", rs.getClobStringOrNull("str_lob"));
        assertArrayEquals("More".getBytes(), rs.getBlobBytesOrNull(8));
        assertArrayEquals("More".getBytes(), rs.getBlobBytesOrNull("bin_blob"));
        assertEquals(currentDate.getTime(), rs.getDateOrNull(9).getTime());
        assertEquals(currentDate.getTime(), rs.getDateOrNull("date_millis").getTime());
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
        + "abs(nbr_float-:f)<0.01 and abs(nbr_double-:d)<0.01 and nbr_big_decimal=:bd and str_varchar=:s and "
        + "date_millis=:date").argInteger("i", 1).argLong("l", 2L).argFloat("f", 3.2f).argDouble("d", 4.2)
        .argBigDecimal("bd", bigDecimal).argString("s", "Hello").argDate("date", currentDate).queryLong());
    List<Long> result = db.select("select count(*) from dbtest where nbr_integer=? and nbr_long=? and "
        + "abs(nbr_float-?)<0.01 and abs(nbr_double-?)<0.01 and nbr_big_decimal=? and str_varchar=? and "
        + "date_millis=?").argInteger(1).argLong(2L).argFloat(3.2f).argDouble(4.2).argBigDecimal(bigDecimal)
        .argString("Hello").argDate(currentDate).queryLongs();
    assertEquals(1, result.size());
    assertEquals(new Long(1), result.get(0));
  }

  @Test
  public void updatePositionalArgs() {
    db.ddl("drop table dbtest").executeQuietly();
    db.ddl("create table dbtest (nbr_integer numeric(8), nbr_long numeric(10), nbr_float numeric(9,3), "
        + "nbr_double numeric(19,9), nbr_big_decimal numeric(25,15), str_varchar varchar(80), str_lob text, "
        + "bin_blob bytea, date_millis timestamp)").execute();
    final Date currentDate = new Date();
    BigDecimal bigDecimal = new BigDecimal("5.3");
    assertEquals(1, db.insert("insert into dbtest values (?,?,?,?,?,?,?,?,?)").argInteger(1).argLong(2L).argFloat(3.2f).argDouble(4.2)
        .argBigDecimal(bigDecimal).argString("Hello").argClobString("World").argBlobBytes("More".getBytes())
        .argDate(currentDate).insert());
    db.update("update dbtest set nbr_integer=?, nbr_long=?, nbr_float=?, nbr_double=?, nbr_big_decimal=?, "
        + "str_varchar=?, str_lob=?, bin_blob=?, date_millis=?").argInteger(null).argLong(null).argFloat(null)
        .argDouble(null).argBigDecimal(null).argString(null).argClobString(null).argBlobBytes(null)
        .argDate(null).update(1);
    db.select("select nbr_integer, nbr_long, nbr_float, nbr_double, nbr_big_decimal, str_varchar, str_lob, "
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
        assertNull(rs.getClobStringOrNull(7));
        assertNull(rs.getClobStringOrNull("str_lob"));
        assertNull(rs.getBlobBytesOrNull(8));
        assertNull(rs.getBlobBytesOrNull("bin_blob"));
        assertNull(rs.getDateOrNull(9));
        assertNull(rs.getDateOrNull("date_millis"));
        return null;
      }
    });
    assertEquals(1, db.update("update dbtest set nbr_integer=?, nbr_long=?, nbr_float=?, nbr_double=?, "
        + "nbr_big_decimal=?, str_varchar=?, str_lob=?, bin_blob=?, date_millis=?").argInteger(1).argLong(2L)
        .argFloat(3.2f).argDouble(4.2).argBigDecimal(bigDecimal).argString("Hello").argClobString("World")
        .argBlobBytes("More".getBytes()).argDate(currentDate).update());
    db.select("select nbr_integer, nbr_long, nbr_float, nbr_double, nbr_big_decimal, str_varchar, str_lob, "
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
        assertEquals("World", rs.getClobStringOrNull(7));
        assertEquals("World", rs.getClobStringOrNull("str_lob"));
        assertArrayEquals("More".getBytes(), rs.getBlobBytesOrNull(8));
        assertArrayEquals("More".getBytes(), rs.getBlobBytesOrNull("bin_blob"));
        assertEquals(currentDate.getTime(), rs.getDateOrNull(9).getTime());
        assertEquals(currentDate.getTime(), rs.getDateOrNull("date_millis").getTime());
        return null;
      }
    });

    db.update("update dbtest set str_lob=?, bin_blob=?").argClobReader(null).argBlobInputStream(null).update(1);
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
        .argBlobInputStream(new ByteArrayInputStream("More".getBytes())).update(1);
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
    db.ddl("drop table dbtest").executeQuietly();
    db.ddl("create table dbtest (nbr_integer numeric(8), nbr_long numeric(10), nbr_float numeric(9,3), "
        + "nbr_double numeric(19,9), nbr_big_decimal numeric(25,15), str_varchar varchar(80), str_lob text, "
        + "bin_blob bytea, date_millis timestamp)").execute();
    final Date currentDate = new Date();
    BigDecimal bigDecimal = new BigDecimal("5.3");
    db.insert("insert into dbtest values (:a,:b,:c,:d,:e,:f,:g,:h,:i)").argInteger(":a", 1).argLong(":b", 2L)
        .argFloat(":c", 3.2f).argDouble(":d", 4.2).argBigDecimal(":e", bigDecimal).argString(":f", "Hello")
        .argClobString(":g", "World").argBlobBytes(":h", "More".getBytes()).argDate(":i", currentDate).insert(1);
    db.update("update dbtest set nbr_integer=:a, nbr_long=:b, nbr_float=:c, nbr_double=:d, nbr_big_decimal=:e, "
        + "str_varchar=:f, str_lob=:g, bin_blob=:h, date_millis=:i").argInteger(":a", null).argLong(":b", null)
        .argFloat(":c", null).argDouble(":d", null).argBigDecimal(":e", null).argString(":f", null)
        .argClobString(":g", null).argBlobBytes(":h", null).argDate(":i", null).update(1);
    db.select("select nbr_integer, nbr_long, nbr_float, nbr_double, nbr_big_decimal, str_varchar, str_lob, "
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
        assertNull(rs.getClobStringOrNull(7));
        assertNull(rs.getClobStringOrNull("str_lob"));
        assertNull(rs.getBlobBytesOrNull(8));
        assertNull(rs.getBlobBytesOrNull("bin_blob"));
        assertNull(rs.getDateOrNull(9));
        assertNull(rs.getDateOrNull("date_millis"));
        return null;
      }
    });
    db.update("update dbtest set nbr_integer=:a, nbr_long=:b, nbr_float=:c, nbr_double=:d, nbr_big_decimal=:e, "
        + "str_varchar=:f, str_lob=:g, bin_blob=:h, date_millis=:i").argInteger(":a", 1).argLong(":b", 2L)
        .argFloat(":c", 3.2f).argDouble(":d", 4.2).argBigDecimal(":e", bigDecimal).argString(":f", "Hello")
        .argClobString(":g", "World").argBlobBytes(":h", "More".getBytes()).argDate(":i", currentDate).update(1);
    db.select("select nbr_integer, nbr_long, nbr_float, nbr_double, nbr_big_decimal, str_varchar, str_lob, "
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
        assertEquals("World", rs.getClobStringOrNull(7));
        assertEquals("World", rs.getClobStringOrNull("str_lob"));
        assertArrayEquals("More".getBytes(), rs.getBlobBytesOrNull(8));
        assertArrayEquals("More".getBytes(), rs.getBlobBytesOrNull("bin_blob"));
        assertEquals(currentDate.getTime(), rs.getDateOrNull(9).getTime());
        assertEquals(currentDate.getTime(), rs.getDateOrNull("date_millis").getTime());
        return null;
      }
    });

    db.update("update dbtest set str_lob=:a, bin_blob=:b").argClobReader(":a", null).argBlobInputStream(":b", null).update(1);
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
        .argBlobInputStream(":b", new ByteArrayInputStream("More".getBytes())).update(1);
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
    db.ddl("drop table dbtest").executeQuietly();
    db.ddl("create table dbtest (nbr_integer numeric(8), nbr_long numeric(10), nbr_float numeric(9,3), "
        + "nbr_double numeric(19,9), nbr_big_decimal numeric(25,15), str_varchar varchar(80), str_lob text, "
        + "bin_blob bytea, date_millis timestamp)").execute();
    db.insert("insert into dbtest values (?,?,?,?,?,?,?,?,?)").argInteger(null).argLong(null).argFloat(null)
        .argDouble(null).argBigDecimal(null).argString(null).argClobString(null).argBlobBytes(null)
        .argDate(null).insert(1);
    db.select("select nbr_integer, nbr_long, nbr_float, nbr_double, nbr_big_decimal, str_varchar, str_lob, "
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
        assertNull(rs.getClobStringOrNull(7));
        assertNull(rs.getClobStringOrNull("str_lob"));
        assertNull(rs.getBlobBytesOrNull(8));
        assertNull(rs.getBlobBytesOrNull("bin_blob"));
        assertNull(rs.getDateOrNull(9));
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
  public void bigClob() {
    db.ddl("drop table dbtest").executeQuietly();
    db.ddl("create table dbtest (str_lob text)").execute();

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
    db.ddl("drop table dbtest").executeQuietly();
    db.ddl("create table dbtest (bin_blob bytea)").execute();

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
