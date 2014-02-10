Getting Started
===============

The point of this project is to provide a simplified way of accessing databases. It is a
wrapper around the JDBC driver, and tries to hide some of the more error-prone and unsafe
parts of the standard API.

Usually the server container will manage creation of the Database or Provider<Database>,
and business logic will declare a dependency on this (e.g. via a constructor parameter).
For example:

    public class MyBusiness {
      private Provider<Database> db;

      public MyBusiness(Provider<Database> db) {
        this.db = db;
      }

      public Long doStuff(String withData) {
        return db.get().select("select count(*) from a where b=:data").argString(withData).queryLong();
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

A few things to note:

* No checked exceptions. All SQLExceptions are wrapped into a DatabaseException that inherits from
  RuntimeException. This makes code much cleaner because in server programming there is usually no
  way to recover from an error (it is handled in a generic way by higher level code).
* No way to control (or mess up) resource handling. Connections, prepared statements, result sets,
  and everything else that requires cleanup or close() calls should be completely handled so there
  is no opportunity for client code to make a mistake and cause resource leaks.
* Type safety. The various argX() calls know the type of the object you intend to pass, and can
  therefore handle null values correctly. No more errors because you pass a null and the JDBC
  driver can't figure out what type it should be.
* Named parameters. You can use traditional positional parameters in the SQL (the '?' character),
  or you can use named parameters (like ":data" above). This can help reduce errors due to counting
  incorrectly. Note you cannot use both positional and named within the same SQL.
* Correct handling of java.util.Date. As long as your database columns have enough precision, Date
  objects will round-trip correctly with millisecond precision. No more fiddling with Timestamp
  and dealing with millisecond truncation and nanoseconds.
* Similarly BigDecimal tries to maintain scale in a more intuitive manner (most drivers will revert
  to "full precision" meaning they pad the scale out to what the database column specifies).