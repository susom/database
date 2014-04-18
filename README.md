# Easier, Safer JDBC

The point of this project is to provide a simplified way of accessing databases. It is a
wrapper around the JDBC driver, and tries to hide some of the more error-prone and unsafe
parts of the standard API.

## Features

* *No checked exceptions.* All SQLExceptions are wrapped into a DatabaseException that inherits from
  RuntimeException. This makes code much cleaner because in server programming there is usually no
  way to recover from an error (it is handled in a generic way by higher level code).
* *No way to control (mess up) resource handling.* Connections, prepared statements, result sets,
  and everything else that requires `close()` calls are hidden so there is no opportunity for client
  code to make a mistake and cause resource leaks.
* *Type safe with null parameters.* The various `argX()` calls know the type of the object you intend to pass, and can
  therefore handle null values correctly. No more errors because you pass a null and the JDBC
  driver can't figure out what type it should be.
* *Indexed or named parameters.* You can use traditional positional parameters in the SQL (the '?' character),
  or you can use named parameters (like ":data" above). This can help reduce errors due to counting
  incorrectly. Note you cannot use both positional and named within the same SQL.
* *Correct handling of java.util.Date.* As long as your database columns have enough precision, Date
  objects will round-trip correctly with millisecond precision. No more fiddling with Timestamp
  and dealing with millisecond truncation and nanoseconds.
* *Correct handling of java.math.BigDecimal.* Similarly BigDecimal tries to maintain scale in a more intuitive manner
  (most drivers will revert to "full precision" meaning they pad the scale out to what the database column specifies).
* *Simplified handling of clobs and blobs.* Deal with them explicitly as either String/byte[] or streams.
  No downcasting or driver-specific APIs, and treat them the same as other parameters.
* *Fewer methods, fewer ways of doing the same thing.* Be ye not overly bewildering, keepers of standards.
* *Central control of instrumentation and logging.* Tracks the important metrics and logs to SLF4J in a way that
  gives you more control than having database-related logging scattered throughout your code.
* *Fluent API that is auto-completion friendly.* Built to make life easier in modern IDEs.

## A Quick Example

Usually the server container will manage creation of the Database or Provider<Database>,
and business logic will declare a dependency on this (e.g. via a constructor parameter).
For example:

```java
public class MyBusiness {
  private Provider<Database> db;

  public MyBusiness(Provider<Database> db) {
    this.db = db;
  }

  public Long doStuff(String data) {
    return db.get().select("select count(*) from a where b=:data").argString(data).queryLong();
  }

  public List<Date> doMoreStuff(Date after) {
    return db.get().select("select my_date from a where b > ?").argDate(after)
           .query(new RowsHandler<List<Date>>() {
              @Override
              public List<Date> process(Rows rs) throws Exception {
                List<Date> result = new ArrayList<>();
                while (rs.next()) {
                  result.add(rs.getDate("my_date"));
                }
                return result;
              }
           }
  }
}
```