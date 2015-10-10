import javax.inject.Provider;

import com.github.susom.database.Database;
import com.github.susom.database.DatabaseProvider;
import com.github.susom.database.DbCode;

/**
 * Example with database info provided from command line. To use this, set properties like this:
 * <br/>
 * <pre>
 *   -Ddatabase.url=...      Database connect string (required)
 *   -Ddatabase.user=...     Authenticate as this user (optional if provided in url)
 *   -Ddatabase.password=... User password (optional if user and password provided in
 *                           url; prompted on standard input if user is provided and
 *                           password is not)
 *   -Ddatabase.flavor=...   What kind of database it is (optional, will guess based
 *                           on the url if this is not provided)
 *   -Ddatabase.driver=...   The Java class of the JDBC driver to load (optional, will
 *                           guess based on the flavor if this is not provided)
 * </pre>
 */
public class HelloAny {
  public void run() {
    DatabaseProvider.fromSystemProperties().transact(new DbCode() {
      @Override
      public void run(Provider<Database> dbp) {
        Database db = dbp.get();
        db.dropTableQuietly("t");
        db.ddl("create table t (a numeric)").execute();
        db.toInsert("insert into t (a) values (?)")
            .argInteger(32)
            .insert(1);
        db.toUpdate("update t set a=:val")
            .argInteger("val", 23)
            .update(1);

        Long rows = db.toSelect("select count(1) from t ").queryLongOrNull();
        System.out.println("Rows: " + rows);
      }
    });
  }

  public static void main(final String[] args) {
    try {
      new HelloAny().run();
    } catch (Exception e) {
      e.printStackTrace();
      System.exit(1);
    }
  }
}
