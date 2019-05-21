/*
 * Copyright 2019 The Board of Trustees of The Leland Stanford Junior University.
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

import com.github.susom.database.*;
import com.github.susom.database.Schema;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.bigquery.*;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.FileReader;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeFalse;
import static org.junit.Assume.assumeTrue;

/**
 * BigQuery support in the library is limited to query.  The currently available JDBC driver has serious limitations
 * that make both DDL and DML problematic.
 *
 * These tests are based on the CommonTest but focus on data type compatibility for the BigQuery data types.
 * BigQuery types (as of 2019-03-04) are:
 * <ul>
 *   <li>INT64 maps to Long</li>
 *   <li>NUMERIC maps to BigDecimal</li>
 *   <li>FLOAT64 maps to Double</li>
 *   <li>BOOL maps to Boolean</li>
 *   <li>STRING maps to String</li>
 *   <li>BYTES maps to byte[]</li>
 *   <li>DATE maps to java.sql.Date</li>
 *   <li>DATETIME maps to java.sql.Timestamp</li>
 *   <li>TIME maps to java.sql.Time</li>
 *   <li>TIMESTAMP maps to java.sql.Timestamp</li>
 * </ul>
 * Additional types that we are not trying to handle are:
 * <ul>
 *   <li>GEOGRAPHY</li>
 *   <li>ARRAY</li>
 *   <li>STRUCT</li>
 * </ul>
 *
 */
public class BigQueryTest extends CommonTest {
  private com.google.cloud.bigquery.BigQuery bigquery;
  private final String datasetName = "bqdbtest";

  @Override
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
    // Delete any existing table
    bigquery.delete(TableId.of(datasetName, "dbtest"));
  }


  @Override
  protected DatabaseProvider createDatabaseProvider(OptionsOverride options) throws Exception {
    Properties properties = new Properties();
    try {
      properties.load(new FileReader(System.getProperty("local.properties", "local.properties")));
    } catch (Exception e) {
      // Don't care, fallback to system properties
    }

    /*
     * Args:
     * 1: project
     * 2: service acct
     * 3: path to key file
     */
    final String defaultUrlTemplate = "jdbc:bigquery://https://www.googleapis.com/bigquery/v2:443;" +
        "ProjectId=%1s;" +
        "DefaultDataset=" + datasetName + ";" +
        "OAuthType=0;" +
        "OAuthServiceAcctEmail=%2s;" +
        "OAuthPvtKeyPath=%3s;";

    final String project =
        System.getProperty("database.bigquery.project", properties.getProperty("database.bigquery.project"));
    final String serviceAcct =
        System.getProperty("database.bigquery.serviceacct", properties.getProperty("database.bigquery.serviceacct"));
    final String keyPath =
        System.getProperty("database.bigquery.keypath", properties.getProperty("database.bigquery.keypath"));

    final String defaultUrl = String.format(defaultUrlTemplate, project, serviceAcct, keyPath);

    bigquery = BigQueryOptions.newBuilder().
        setProjectId(project).
        setCredentials(GoogleCredentials.fromStream(new FileInputStream(keyPath))).
        build().getService();

    return DatabaseProvider.fromDriverManager(
        System.getProperty("database.url", properties.getProperty("database.url", defaultUrl)),
        System.getProperty("database.user", properties.getProperty("database.user")),
        System.getProperty("database.password", properties.getProperty("database.password"))
    ).withSqlParameterLogging().withSqlInExceptionMessages().withOptions(options).create();
  }

  @Override
  @Test
  public void selectNewTable() {
//    new Schema()
//        .addTable("dbtest")
//        .addColumn("nbr_integer").asInteger()/*.primaryKey()*/.table()
//        .addColumn("nbr_long").asLong().table()
//        .addColumn("nbr_float").asFloat().table()
//        .addColumn("nbr_double").asDouble().table()
//        .addColumn("nbr_big_decimal").asBigDecimal(19, 9).table()
//        .addColumn("str_varchar").asString(80).table()
//        .addColumn("str_fixed").asStringFixed(1).table()
//        .addColumn("str_lob").asClob().table()
//        .addColumn("bin_blob").asBlob().table()
//        .addColumn("date_millis").asDate().table().schema().execute(db);

    // Create new table
    com.google.cloud.bigquery.Schema schema = com.google.cloud.bigquery.Schema.of(
        Field.of("nbr_integer", LegacySQLTypeName.INTEGER),
        Field.of("nbr_long", LegacySQLTypeName.INTEGER),
        Field.of("nbr_float", LegacySQLTypeName.FLOAT),
        Field.of("nbr_double", LegacySQLTypeName.FLOAT),
        Field.of("nbr_big_decimal", LegacySQLTypeName.NUMERIC),
        Field.of("str_varchar", LegacySQLTypeName.STRING),
        Field.of("str_fixed", LegacySQLTypeName.STRING),
        Field.of("str_lob", LegacySQLTypeName.STRING),
        Field.of("bin_blob", LegacySQLTypeName.BYTES),
        Field.of("date_millis", LegacySQLTypeName.DATETIME)
    );
    com.google.cloud.bigquery.Table table = bigquery.create(TableInfo.of(TableId.of(datasetName, "dbtest"), StandardTableDefinition.of(schema)));
    assumeTrue(table.exists());

    BigDecimal bigDecimal = new BigDecimal("5.3");
    Map<String, Object> insertContent = new HashMap<>();
    insertContent.put("nbr_integer", 1);
    insertContent.put("nbr_long", 2L);
    insertContent.put("nbr_float", 3.2F);
    insertContent.put("nbr_double", 4.2);
    insertContent.put("nbr_big_decimal", bigDecimal);
    insertContent.put("str_varchar", "Hello");
    insertContent.put("str_fixed", "T");
    insertContent.put("str_lob", "World");
    insertContent.put("bin_blob", Base64.getEncoder().encodeToString("More".getBytes()));
    LocalDate localDate = LocalDate.of(2019, 2, 17);
    Date date = Date.from(localDate.atStartOfDay(ZoneId.systemDefault()).toInstant());
    insertContent.put("date_millis", DateTimeFormatter.ISO_DATE.format(localDate));
    InsertAllRequest insertAllRequest =
        InsertAllRequest.newBuilder(table, InsertAllRequest.RowToInsert.of(insertContent)).
            setIgnoreUnknownValues(false).setSkipInvalidRows(false).build();
    assumeFalse(insertAllRequest.skipInvalidRows() || insertAllRequest.ignoreUnknownValues());
    InsertAllResponse insertAllResponse = bigquery.insertAll(insertAllRequest);
    assumeFalse(insertAllResponse.hasErrors());

    db.toSelect(
        "select nbr_integer, nbr_long, nbr_float, nbr_double, nbr_big_decimal, str_varchar, str_fixed, str_lob, "
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
        assertEquals(Date.from(localDate.atStartOfDay().atZone(ZoneId.systemDefault()).toInstant()), rs.getDateOrNull(10));
        assertEquals(date, rs.getDateOrNull("date_millis"));
        return null;
      }
    });
    // Repeat the above query, using the various methods that automatically infer the column
    db.toSelect(
        "select nbr_integer, nbr_long, nbr_float, nbr_double, nbr_big_decimal, str_varchar, str_fixed, str_lob, "
            + "bin_blob, date_millis from dbtest").query(new RowsHandler<Void>() {
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
        assertEquals(date, rs.getDateOrNull());
        return null;
      }
    });
    db.toSelect(
        "select nbr_integer, nbr_long, nbr_float, nbr_double, nbr_big_decimal, str_varchar, str_fixed, str_lob, "
            + "bin_blob, date_millis from dbtest").query(new RowsHandler<Void>() {
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
        + "and str_fixed=:sf and date_millis=:date").argInteger("i", 1).argLong("l", 2L).argFloat("f", 3.2f)
        .argDouble("d", 4.2).argBigDecimal("bd", bigDecimal).argString("s", "Hello").argString("sf", "T")
        .argDate("date", now).queryLongOrNull());
    List<Long> result = db.toSelect("select count(*) from dbtest where nbr_integer=:i and nbr_long=:l and "
        + "abs(nbr_float-:f)<0.01 and abs(nbr_double-:d)<0.01 and nbr_big_decimal=:bd and str_varchar=:s "
        + "and str_fixed=:sf and date_millis=:date").argInteger("i", 1).argLong("l", 2L).argFloat("f", 3.2f)
        .argDouble("d", 4.2).argBigDecimal("bd", bigDecimal).argString("s", "Hello").argString("sf", "T")
        .argDate("date", now).queryLongs();
    assertEquals(1, result.size());
    assertEquals(new Long(1), result.get(0));
  }

}
