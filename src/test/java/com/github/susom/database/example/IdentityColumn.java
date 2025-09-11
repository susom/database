package com.github.susom.database.example;

import com.github.susom.database.Database;
import com.github.susom.database.Schema;

/**
 * Demo of using identity columns as a safer alternative to sequences.
 */
public class IdentityColumn extends DerbyExample {
  void example(Database db, String[] args) {
    // Drops in case we are running this multiple times
    db.dropTableQuietly("t");

    // Create a table with an identity column (no sequence needed!)
    new Schema()
        .addTable("t")
          .addColumn("pk").primaryKeyIdentity().table()
          .addColumn("d").asDate().table()
          .addColumn("s").asString(80).schema()
        .execute(db);

    // Insert a row into the table using identity column.
    // Note: No argPkSeq() call needed - the database automatically generates the key!
    // This eliminates the risk of specifying wrong sequence names.
    Long pk = db.toInsert(
        "insert into t (d,s) values (?,?)")
        .argDateNowPerDb()
        .argString("Hello Identity!")
        .insertReturningPkDefault("pk");

    println("Inserted row with auto-generated pk=" + pk);
    
    // Insert another row to demonstrate incrementing
    Long pk2 = db.toInsert(
        "insert into t (d,s) values (?,?)")
        .argDateNowPerDb()
        .argString("Another record")
        .insertReturningPkDefault("pk");

    println("Inserted second row with auto-generated pk=" + pk2);
  }

  public static void main(String[] args) {
    new IdentityColumn().launch(args);
  }
}