import com.github.susom.database.Database;
import com.github.susom.database.Schema;
import com.github.susom.database.Sql;

/**
 * Demo of how to use the Sql helper class to dynamically build queries.
 */
public class DynamicSql extends DerbyExample {
  void example(String[] args, Database db) {
    // Drops in case we are running this multiple times
    db.dropTableQuietly("t");

    // Create and populate a simple table
    new Schema()
        .addTable("t")
          .addColumn("pk").primaryKey().table()
          .addColumn("s").asString(80).schema().execute(db);
    db.toInsert("insert into t (pk,s) values (?,?)")
        .argLong(1L).argString("Hi").insert(1);
    db.toInsert("insert into t (pk,s) values (?,?)")
        .argLong(2L).argString("Hi").insert(1);

    // Construct various dynamic queries and execute them
    System.out.println("Rows with none: " + countByPkOrS(db, null, null));
    System.out.println("Rows with pk=1: " + countByPkOrS(db, 1L, null));
    System.out.println("Rows with s=Hi: " + countByPkOrS(db, null, "Hi"));
  }

  Long countByPkOrS(Database db, Long pk, String s) {
    Sql sql = new Sql("select count(*) from t");

    boolean where = true;
    if (pk != null) {
      where = false;
      sql.append(" where pk=?").argLong(pk);
    }
    if (s != null) {
      if (where) {
        sql.append(" where ");
      } else {
        sql.append(" and ");
      }
      sql.append("s=?").argString(s);
    }

    return db.toSelect(sql).queryLongOrNull();
  }

  public static void main(String[] args) {
    new DynamicSql().launch(args);
  }
}
