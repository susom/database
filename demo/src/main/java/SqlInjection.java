import com.github.susom.database.Database;
import com.github.susom.database.Sql;

/**
 * Demo of using the checker framework to detect SQL injections. To see how
 * this works, download The Checker Framework (http://types.cs.washington.edu/checker-framework/),
 * adjust the checker.dir property in pom.xml, and execute this Maven command:
 *
 * <p>mvn -Pchecker -Dchecker.dir=/checker/install/dir</p>
 */
public class SqlInjection extends DerbyExample {
  void example(String[] args, Database db) {
    String tainted = args[0];

    // Checker will flag each of these as a type error
    System.out.println(db.toSelect(tainted).queryLongOrNull());
    db.toInsert(tainted).insert(1);
    db.toUpdate(tainted).update(1);
    db.toDelete(tainted).update(1);

    Sql sql = new Sql(tainted);
    sql.append(tainted);

    // These two lines are actually ok, since the Sql class should be untainted
    db.toInsert(sql.sql()).insert(1);
    System.out.println(db.toSelect(sql).queryLongOrNull());

    db.toInsert(someSql().sql()).insert(1);
    db.toInsert("" + db.when()).insert(1);
    db.toInsert("" + db.when().derby("a")).insert(1);
    db.toInsert(db.when().derby("a").other("b")).insert(1);
    db.toInsert(db.when().derby(tainted).other("")).insert(1);
  }

  Sql someSql() {
    return new Sql().append("bar" + "1").argString("baz");
  }

  public static void main(String[] args) {
    new DynamicSql().launch(args);
  }
}
