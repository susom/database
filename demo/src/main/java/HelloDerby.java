import com.github.susom.database.Database;
import com.github.susom.database.DatabaseProvider;
import com.github.susom.database.DbRun;
import com.github.susom.database.Rows;
import com.github.susom.database.RowsHandler;

import java.io.File;

import javax.inject.Provider;

import org.apache.log4j.Logger;
import org.apache.log4j.xml.DOMConfigurator;

/**
 * Demo of using some com.github.susom.database classes with Derby.
 */
public class HelloDerby {
  public void run() {
    // Put all Derby related files inside ./build to keep our working copy clean
    File directory = new File("target").getAbsoluteFile();
    if (directory.exists() || directory.mkdirs()) {
      System.setProperty("derby.stream.error.file", new File(directory, "derby.log").getAbsolutePath());
    }

    String url = "jdbc:derby:target/testdb;create=true";
    DatabaseProvider.fromDriverManager(url).transactRollbackOnError(new DbRun() {
      @Override
      public void run(Provider<Database> dbp) {
        Database db = dbp.get();
        db.ddl("drop table t").executeQuietly();
        db.ddl("create table t (a numeric)").execute();
        db.toInsert("insert into t (a) values (?)").argInteger(32).insert(1);
        db.toUpdate("update t set a=:val")
            .argInteger("val", 23)
            .update(1);

        Long rows = db.toSelect("select count(1) from t ").queryLongOrNull();
        println("Rows: " + rows);
      }
    });
  }

  public void println(String s) {
    System.out.println(s);
  }

  public static void main(final String[] args) {
    try {
      new HelloDerby().run();
    } catch (Exception e) {
      e.printStackTrace();
      System.exit(1);
    }
  }
}
