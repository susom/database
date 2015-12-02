import javax.inject.Provider;

import com.github.susom.database.Database;
import com.github.susom.database.DatabaseException;
import com.github.susom.database.DatabaseProvider;
import com.github.susom.database.DatabaseProvider.Builder;
import com.github.susom.database.DbCode;
import com.github.susom.database.DbCodeTx;
import com.github.susom.database.DbRun;
import com.github.susom.database.Schema;
import com.github.susom.database.Transaction;

/**
 * Demo of how to use the {@code DatabaseProvider.fakeBuilder()} to control
 * transactions for testing purposes.
 */
public class FakeBuilder extends DerbyExample {
  void example(Builder dbb, String[] args) {
    DatabaseProvider realDbp = null;

    try {
      realDbp = dbb.create();

      dbb.transact(new DbCode() {
        @Override
        public void run(Provider<Database> db) throws Exception {
          // Drops in case we are running this multiple times
          db.get().dropTableQuietly("t");

          // Create and populate a simple table
          new Schema().addTable("t").addColumn("pk").primaryKey().schema().execute(db.get());
        }
      });

      Builder fakeBuilder = realDbp.fakeBuilder();

      // Trying all three transact methods, just for completeness
      fakeBuilder.transact(new DbCode() {
        @Override
        public void run(Provider<Database> db) throws Exception {
          db.get().toInsert("insert into t (pk) values (?)").argLong(1L).insert(1);
        }
      });
      fakeBuilder.transact(new DbCodeTx() {
        @Override
        public void run(Provider<Database> db, Transaction tx) throws Exception {
          db.get().toInsert("insert into t (pk) values (?)").argLong(2L).insert(1);
        }
      });
      fakeBuilder.transact(new DbRun() {
        @Override
        public void run(Provider<Database> db) throws Exception {
          db.get().toInsert("insert into t (pk) values (?)").argLong(3L).insert(1);
        }
      });

      fakeBuilder.transact(new DbCode() {
        @Override
        public void run(Provider<Database> db) throws Exception {
          println("Rows before rollback: " + db.get().toSelect("select count(*) from t").queryLongOrZero());
        }
      });

      realDbp.rollbackAndClose();

      // Can't use fakeBuilder after close
      try {
        fakeBuilder.transact(new DbCode() {
          @Override
          public void run(Provider<Database> db) throws Exception {
            db.get();
            println("Eeek...shouldn't get here!");
          }
        });
      } catch(DatabaseException e) {
        println("Correctly threw exception: " + e.getMessage());
      }

      dbb.transact(new DbCode() {
        @Override
        public void run(Provider<Database> db) throws Exception {
          println("Rows after rollback: " + db.get().toSelect("select count(*) from t").queryLongOrZero());
        }
      });
    } finally {
      if (realDbp != null) {
        realDbp.rollbackAndClose();
      }
    }
  }

  public static void main(String[] args) {
    new FakeBuilder().launch(args);
  }
}
