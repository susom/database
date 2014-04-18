package com.github.susom.database.test;

import java.io.FileReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;

/**
 * Exercise Database functionality with a real Oracle database.
 *
 * @author garricko
 */
public class OracleTest extends CommonTest {
  @Override
  protected Connection createConnection() throws Exception {
    Properties properties = new Properties();
    try {
      properties.load(new FileReader(System.getProperty("build.properties", "../build.properties")));
    } catch (Exception e) {
      // Don't care, fallback to system properties
    }

    return DriverManager.getConnection(System.getProperty("database.url", properties.getProperty("database.url")),
        System.getProperty("database.user", properties.getProperty("database.user")),
        System.getProperty("database.password", properties.getProperty("database.password")));
  }
}
