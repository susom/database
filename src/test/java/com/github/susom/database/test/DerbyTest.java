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
import java.sql.Connection;
import java.sql.DriverManager;

/**
 * Exercise Database functionality with a real database (Derby).
 *
 * @author garricko
 */
public class DerbyTest extends CommonTest {
  static {
    // We will put all Derby related files inside ./build to keep our working copy clean
    File directory = new File("build").getAbsoluteFile();
    if (directory.exists() || directory.mkdirs()) {
      System.setProperty("derby.stream.error.file", new File(directory, "derby.log").getAbsolutePath());
    }
  }

  @Override
  protected Connection createConnection() throws Exception {
    // For embedded Derby database
    return DriverManager.getConnection("jdbc:derby:build/testdb;create=true");
  }
}
