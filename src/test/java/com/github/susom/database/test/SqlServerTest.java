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

import org.junit.Ignore;
import org.junit.Test;

import com.github.susom.database.Config;
import com.github.susom.database.ConfigFrom;
import com.github.susom.database.DatabaseProvider;
import com.github.susom.database.OptionsOverride;
import com.github.susom.database.Schema;

import static org.junit.Assert.assertArrayEquals;

/**
 * Exercise Database functionality with a real Oracle database.
 *
 * @author garricko
 */
public class SqlServerTest extends CommonTest {
  @Override
  protected DatabaseProvider createDatabaseProvider(OptionsOverride options) throws Exception {
    String propertiesFile = System.getProperty("local.properties", "local.properties");
    Config config = ConfigFrom.firstOf()
        .systemProperties()
        .propertyFile(propertiesFile)
        .excludePrefix("database.")
        .removePrefix("sqlserver.").get();
    return DatabaseProvider.fromDriverManager(config)
        .withSqlParameterLogging()
        .withSqlInExceptionMessages()
        .withOptions(options).create();
  }

  @Ignore("SQL Server prohibits NaN and Infinity")
  @Test
  public void argFloatNaN() {
    super.argFloatNaN();
  }

  @Ignore("SQL Server prohibits NaN and Infinity")
  @Test
  public void argFloatInfinity() {
    super.argFloatInfinity();
  }

  @Ignore("SQL Server prohibits NaN and Infinity")
  @Test
  public void argDoubleNaN() {
    super.argDoubleNaN();
  }

  @Ignore("SQL Server prohibits NaN and Infinity")
  @Test
  public void argDoubleInfinity() {
    super.argDoubleInfinity();
  }

  @Ignore("SQL Server seems to have incorrect min value for float (rounds to zero)")
  @Test
  public void argFloatMinMax() {
    super.argFloatMinMax();
  }

  @Ignore("SQL Server doesn't support the interval syntax for date arithmetic")
  @Test
  public void intervals() {
    super.intervals();
  }

  /**
   * SQL Server seems to have different behavior in that is does not convert
   * column names to uppercase (it preserves the case).
   * I haven't figured out how to smooth over this difference, since all databases
   * seem to respect the provided case when it is inside quotes, but don't provide
   * a way to tell whether a particular parameter was quoted.
   */
  @Override
  @Test
  public void metadataColumnNames() {
    db.dropTableQuietly("dbtest");

    new Schema().addTable("dbtest").addColumn("pk").primaryKey().schema().execute(db);

    db.toSelect("select Pk, Pk as Foo, Pk as \"Foo\" from dbtest").query(rs -> {
      assertArrayEquals(new String[] { "Pk", "Foo", "Foo" }, rs.getColumnLabels());
      return null;
    });
  }
}
