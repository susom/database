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

import java.io.File;
import java.math.BigDecimal;

import org.junit.Ignore;
import org.junit.Test;

import com.github.susom.database.DatabaseProvider;
import com.github.susom.database.OptionsOverride;
import com.github.susom.database.Schema;

import static org.junit.Assert.assertEquals;

/**
 * Exercise Database functionality with a real database (Derby).
 *
 * @author garricko
 */
public class DerbyTest extends CommonTest {
  static {
    // We will put all Derby related files inside ./build to keep our working copy clean
    File directory = new File("target").getAbsoluteFile();
    if (directory.exists() || directory.mkdirs()) {
      System.setProperty("derby.stream.error.file", new File(directory, "derby.log").getAbsolutePath());
    }
  }

  @Override
  protected DatabaseProvider createDatabaseProvider(OptionsOverride options) throws Exception {
    return DatabaseProvider.fromDriverManager("jdbc:derby:target/testdb;create=true")
        .withSqlParameterLogging().withSqlInExceptionMessages().withOptions(options).create();
  }

  @Ignore("Derby prohibits NaN and Infinity (https://issues.apache.org/jira/browse/DERBY-3290)")
  @Test
  public void argFloatNaN() {
    super.argFloatNaN();
  }

  @Ignore("Derby prohibits NaN and Infinity (https://issues.apache.org/jira/browse/DERBY-3290)")
  @Test
  public void argFloatInfinity() {
    super.argFloatInfinity();
  }

  @Ignore("Derby prohibits NaN and Infinity (https://issues.apache.org/jira/browse/DERBY-3290)")
  @Test
  public void argDoubleNaN() {
    super.argDoubleNaN();
  }

  @Ignore("Derby prohibits NaN and Infinity (https://issues.apache.org/jira/browse/DERBY-3290)")
  @Test
  public void argDoubleInfinity() {
    super.argDoubleInfinity();
  }

  @Ignore("Current Derby behavior is to convert -0f to 0f")
  @Test
  public void argFloatNegativeZero() {
    super.argFloatNegativeZero();
  }

  @Ignore("Current Derby behavior is to convert -0d to 0d")
  @Test
  public void argDoubleNegativeZero() {
    super.argDoubleNegativeZero();
  }
  @Test
  public void argBigDecimal31Precision0() {
    db.dropTableQuietly("dbtest");

    new Schema().addTable("dbtest").addColumn("i").asBigDecimal(31, 0).schema().execute(db);

    BigDecimal value = new BigDecimal("9999999999999999999999999999999"); // 31 digits
    db.toInsert("insert into dbtest (i) values (?)").argBigDecimal(value).insert(1);
    assertEquals(value,
        db.toSelect("select i from dbtest where i=?").argBigDecimal(value).queryBigDecimalOrNull());
  }

  @Test
  public void argBigDecimal31Precision1() {
    db.dropTableQuietly("dbtest");

    new Schema().addTable("dbtest").addColumn("i").asBigDecimal(31, 1).schema().execute(db);

    BigDecimal value = new BigDecimal("999999999999999999999999999999.9"); // 31 digits
    db.toInsert("insert into dbtest (i) values (?)").argBigDecimal(value).insert(1);
    assertEquals(value,
        db.toSelect("select i from dbtest where i=?").argBigDecimal(value).queryBigDecimalOrNull());
  }

  @Test
  public void argBigDecimal31Precision30() {
    db.dropTableQuietly("dbtest");

    new Schema().addTable("dbtest").addColumn("i").asBigDecimal(31, 30).schema().execute(db);

    BigDecimal value = new BigDecimal("9.999999999999999999999999999999"); // 31 digits
    db.toInsert("insert into dbtest (i) values (?)").argBigDecimal(value).insert(1);
    assertEquals(value,
        db.toSelect("select i from dbtest where i=?").argBigDecimal(value).queryBigDecimalOrNull());
  }

  @Test
  public void argBigDecimal31Precision31() {
    db.dropTableQuietly("dbtest");

    new Schema().addTable("dbtest").addColumn("i").asBigDecimal(31, 31).schema().execute(db);

    BigDecimal value = new BigDecimal("0.9999999999999999999999999999999"); // 31 digits
    db.toInsert("insert into dbtest (i) values (?)").argBigDecimal(value).insert(1);
    System.out.println(db.toSelect("select i from dbtest").queryBigDecimalOrNull());
    assertEquals(value,
        db.toSelect("select i from dbtest where i=?").argBigDecimal(value).queryBigDecimalOrNull());
  }

  @Ignore("Derby limits out at precision 31")
  @Test
  public void argBigDecimal38Precision0() {
    super.argBigDecimal38Precision0();
  }

  @Ignore("Derby limits out at precision 31")
  @Test
  public void argBigDecimal38Precision1() {
    super.argBigDecimal38Precision1();
  }

  @Ignore("Derby limits out at precision 31")
  @Test
  public void argBigDecimal38Precision37() {
    super.argBigDecimal38Precision37();
  }

  @Ignore("Derby limits out at precision 31")
  @Test
  public void argBigDecimal38Precision38() {
    super.argBigDecimal38Precision38();
  }
}
