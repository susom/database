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
