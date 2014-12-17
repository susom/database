import com.github.susom.database.Database;
import com.github.susom.database.Schema;

/**
 * Demo of using some com.github.susom.database classes with Derby.
 */
public class InsertReturning extends DerbyExample {
  void example(Database db, String[] args) {
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

    println("Inserted row with pk=" + pk);
  }

  public static void main(String[] args) {
    new InsertReturning().launch(args);
  }
}
