package com.github.susom.database.example;

import java.io.File;

import com.github.susom.database.Database;
import com.github.susom.database.DatabaseProvider;
import com.github.susom.database.DatabaseProvider.Builder;

/**
 * Demo of using some com.github.susom.database classes with Derby.
 */
public abstract class DerbyExample {
  void example(Database db, String[] args) {
    // For subclasses to override
  }

  void example(Builder dbb, final String[] args) {
    dbb.transact(db -> {
      example(db.get(), args);
    });
  }

  public void println(String s) {
    System.out.println(s);
  }

  public final void launch(final String[] args) {
    try {
      // Put all Derby related files inside ./target to keep our working copy clean
      File directory = new File("target").getAbsoluteFile();
      if (directory.exists() || directory.mkdirs()) {
        System.setProperty("derby.stream.error.file", new File(directory, "derby.log").getAbsolutePath());
      }

      String url = "jdbc:derby:target/testdb;create=true";
      example(DatabaseProvider.fromDriverManager(url), args);
    } catch (Exception e) {
      e.printStackTrace();
      System.exit(1);
    }
  }
}
