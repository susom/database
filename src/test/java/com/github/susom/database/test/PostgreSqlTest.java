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

import java.io.FileReader;
import java.util.Properties;

import org.junit.Test;

import com.github.susom.database.DatabaseProvider;
import com.github.susom.database.OptionsOverride;
import com.github.susom.database.Rows;
import com.github.susom.database.RowsHandler;
import com.github.susom.database.Schema;

import static org.junit.Assert.assertArrayEquals;

/**
 * Exercise Database functionality with a real PostgreSQL database.
 *
 * @author garricko
 */
public class PostgreSqlTest extends CommonTest {
  @Override
  protected DatabaseProvider createDatabaseProvider(OptionsOverride options) throws Exception {
    Properties properties = new Properties();
    try {
      properties.load(new FileReader(System.getProperty("build.properties", "../build.properties")));
    } catch (Exception e) {
      // Don't care, fallback to system properties
    }

    Class.forName("org.postgresql.Driver");

    return DatabaseProvider.fromDriverManager(
        System.getProperty("postgres.database.url", properties.getProperty("postgres.database.url")),
        System.getProperty("postgres.database.user", properties.getProperty("postgres.database.user")),
        System.getProperty("postgres.database.password", properties.getProperty("postgres.database.password"))
    ).withOptions(options).withSqlParameterLogging().withSqlInExceptionMessages().create();
  }

  /**
   * PostgreSQL seems to have different behavior in that is does not convert
   * column names to uppercase (it actually converts them to lowercase).
   * I haven't figured out how to smooth over this difference, since all databases
   * seem to respect the provided case when it is inside quotes, but don't provide
   * a way to tell whether a particular parameter was quoted.
   */
  @Override
  @Test
  public void metadataColumnNames() {
    db.dropTableQuietly("dbtest");

    new Schema().addTable("dbtest").addColumn("pk").primaryKey().schema().execute(db);

    db.toSelect("select Pk, Pk as Foo, Pk as \"Foo\" from dbtest")
        .query(new RowsHandler<Object>() {
      @Override
      public Object process(Rows rs) throws Exception {
        assertArrayEquals(new String[] { "pk", "foo", "Foo" }, rs.getColumnNames());
        return null;
      }
    });
  }
}
