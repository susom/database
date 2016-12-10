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

import com.github.susom.database.Config;
import com.github.susom.database.ConfigFrom;
import com.github.susom.database.DatabaseProvider;
import com.github.susom.database.OptionsOverride;

/**
 * Exercise database functionality with a real MySQL database.
 *
 * @author garricko
 */
public class MySqlTest extends CommonTest {
  @Override
  protected DatabaseProvider createDatabaseProvider(OptionsOverride options) throws Exception {
    String propertiesFile = System.getProperty("local.properties", "local.properties");
    Config config = ConfigFrom.firstOf()
        .systemProperties()
        .propertyFile(propertiesFile)
        .excludePrefix("database.")
        .removePrefix("mysql.").get();
    return DatabaseProvider.fromDriverManager(config)
        .withSqlParameterLogging()
        .withSqlInExceptionMessages()
        .withOptions(options).create();
  }
}
