import java.io.File;

import javax.inject.Provider;

import org.apache.log4j.Logger;
import org.apache.log4j.xml.DOMConfigurator;

import com.github.susom.database.Database;
import com.github.susom.database.DatabaseProvider;
import com.github.susom.database.DbRun;
import com.github.susom.database.Schema;

/**
 * Demo of using some com.github.susom.database classes with Derby.
 */
public class InsertReturning {

  private static void theInterestingBit(Database db) {
    // Drops in case we are running this multiple times
    db.dropTableQuietly("t");
    db.dropSequenceQuietly("pk_seq");

    // Create a table and a sequence
    new Schema()
        .addTable("t")
          .addColumn("pk").primaryKey().table()
          .addColumn("d").asDate().table()
          .addColumn("s").asString(80).schema()
        .addSequence("pk_seq").schema().execute(db);

    // Insert a row into the table, populating the primary key from a sequence,
    // and the date based on current database time. Observe that this will work
    // on Derby, where it results in a query for the sequence value, followed by
    // the insert. On databases like Oracle, this will be optimized into a single
    // statement that does the insert and also returns the primary key.
    Long pk = db.toInsert(
        "insert into t (pk,d,s) values (?,?,?)")
        .argPkSeq("pk_seq")
        .argDateNowPerDb()
        .argString("Hi")
        .insertReturningPkSeq("pk");

    System.out.println("Inserted row with pk=" + pk);
  }

  public static void main(String[] args) {
    try {
      // Initialize logging
      String log4jConfig = new File("log4j.xml").getAbsolutePath();
      DOMConfigurator.configure(log4jConfig);
      Logger log = Logger.getLogger(InsertReturning.class);
      log.info("Initialized log4j using file: " + log4jConfig);

      // Put all Derby related files inside ./build to keep our working copy clean
      File directory = new File("target").getAbsoluteFile();
      if (directory.exists() || directory.mkdirs()) {
        System.setProperty("derby.stream.error.file", new File(directory, "derby.log").getAbsolutePath());
      }

      String url = "jdbc:derby:target/testdb;create=true";
      DatabaseProvider.fromDriverManager(url).transact(new DbRun() {
        @Override
        public void run(Provider<Database> db) {
          theInterestingBit(db.get());
        }
      });
    } catch (Exception e) {
      e.printStackTrace();
      System.exit(1);
    }
  }
}
