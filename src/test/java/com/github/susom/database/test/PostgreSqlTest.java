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

import com.github.susom.database.DatabaseProvider;
import com.github.susom.database.OptionsOverride;

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
    ).withDetailedLoggingAndExceptions().create();
  }
}
