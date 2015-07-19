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

import org.junit.Ignore;
import org.junit.Test;

import com.github.susom.database.DatabaseProvider;
import com.github.susom.database.OptionsOverride;

/**
 * Exercise Database functionality with a real Oracle database.
 *
 * @author garricko
 */
public class OracleTest extends CommonTest {
  @Override
  protected DatabaseProvider createDatabaseProvider(OptionsOverride options) throws Exception {
    Properties properties = new Properties();
    try {
      properties.load(new FileReader(System.getProperty("local.properties", "local.properties")));
    } catch (Exception e) {
      // Don't care, fallback to system properties
    }

    return DatabaseProvider.fromDriverManager(
        System.getProperty("database.url", properties.getProperty("database.url")),
        System.getProperty("database.user", properties.getProperty("database.user")),
        System.getProperty("database.password", properties.getProperty("database.password"))
    ).withSqlParameterLogging().withSqlInExceptionMessages().withOptions(options).create();
  }

  @Ignore("Current Oracle behavior is to convert -0f to 0f")
  @Test
  public void argFloatNegativeZero() {
    super.argFloatNegativeZero();
  }

  @Ignore("Current Oracle behavior is to convert -0d to 0d")
  @Test
  public void argDoubleNegativeZero() {
    super.argDoubleNegativeZero();
  }
}
