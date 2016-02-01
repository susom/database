import java.io.File;

import com.github.susom.database.Config;
import com.github.susom.database.DatabaseProviderVertx;
import com.github.susom.database.DatabaseProviderVertx.Builder;
import com.github.susom.database.Schema;

import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;

/**
 * Demo of using some com.github.susom.database classes with Vertx and Derby.
 */
public class VertxServer {
  public void run() {
    // A JSON config you might get from Vertx
    JsonObject jsonConfig = new JsonObject()
        .put("database.url", "jdbc:derby:target/testdb;create=true");

    // Set up Vertx and database access
    Vertx vertx = Vertx.vertx();
    Config config = Config.from().custom(jsonConfig::getString).get();
//    Config config = Config.from().custom(VertxConfig.json(jsonConfig)).get();
    Builder dbb = DatabaseProviderVertx.builder(vertx, config)
        .withSqlInExceptionMessages()
        .withSqlParameterLogging();

    // Set up a table with some data we can query later
    dbb.transact(db -> {
      db.get().dropTableQuietly("t");
      new Schema().addTable("t")
          .addColumn("pk").primaryKey().table()
          .addColumn("message").asString(80).schema().execute(db);
      db.get().toInsert("insert into t (pk,message) values (?,?)")
          .argInteger(1).argString("Hello World!").insert(1);
      db.get().toInsert("insert into t (pk,message) values (?,?)")
          .argInteger(2).argString("Goodbye!").insert(1);
    });

    vertx.createHttpServer().requestHandler(rc -> {
      // Read the query parameter
      int pk = Integer.parseInt(rc.getParam("pk"));

      // Lookup the message from the database
      dbb.transactAsync(db -> {
        // Note this part happens on a worker thread
        return db.get().toSelect("select message from t where pk=?").argInteger(pk).queryStringOrEmpty();
      }, result -> {
        // Now we are back on the event loop thread
        if (result.succeeded()) {
          rc.response().setStatusCode(200).end(result.result());
        } else {
          rc.response().setStatusCode(500).end(result.cause().toString());
        }
      });
    }).listen(8123, result ->
        println("Started server. Go to http://localhost:8123/?pk=1 or http://localhost:8123/?pk=2")
    );
  }

  public void println(String s) {
    System.out.println(s);
  }

  public static void main(final String[] args) {
    try {
      // Put all Derby related files inside ./target to keep our working copy clean
      File directory = new File("target").getAbsoluteFile();
      if (directory.exists() || directory.mkdirs()) {
        System.setProperty("derby.stream.error.file", new File(directory, "derby.log").getAbsolutePath());
      }

      new VertxServer().run();
    } catch (Exception e) {
      e.printStackTrace();
      System.exit(1);
    }
  }
}
