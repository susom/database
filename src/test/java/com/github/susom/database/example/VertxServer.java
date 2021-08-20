package com.github.susom.database.example;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.susom.database.Config;
import com.github.susom.database.ConfigFrom;
import com.github.susom.database.DatabaseProviderVertx;
import com.github.susom.database.DatabaseProviderVertx.Builder;
import com.github.susom.database.Metric;
import com.github.susom.database.Schema;

import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;

/**
 * Demo of using some com.github.susom.database classes with Vertx and HyperSQL.
 */
public class VertxServer {
  private static final Logger log = LoggerFactory.getLogger(VertxServer.class);
  private final Object lock = new Object();

  public void run() throws Exception {
    // A JSON config you might get from Vertx. In a real scenario you would
    // also set database.user, database.password and database.pool.size.
    JsonObject jsonConfig = new JsonObject()
        .put("database.url", "jdbc:hsqldb:file:target/hsqldb;shutdown=true");

    // Set up Vertx and database access
    Vertx vertx = Vertx.vertx();
    Config config = ConfigFrom.firstOf().custom(jsonConfig::getString).get();
    Builder dbb = DatabaseProviderVertx.pooledBuilder(vertx, config)
        .withSqlInExceptionMessages()
        .withSqlParameterLogging();

    // Set up a table with some data we can query later
    dbb.transact(db -> {
      db.get().dropTableQuietly("t");
      new Schema().addTable("t")
          .addColumn("pk").primaryKey().table()
          .addColumn("message").asString(80).schema().execute(db);

      db.get().toInsert("insert into t (pk,message) values (?,?)")
          .argInteger(1).argString("Hello World!").batch()
          .argInteger(2).argString("Goodbye!").insertBatch();
    });

    // Start our server
    vertx.createHttpServer().requestHandler(request -> {
      Metric metric = new Metric(true);

      // Read the query parameter from the request
      String pkParam = request.getParam("pk");
      if (pkParam == null) {
        // Probably a favicon or similar request we ignore for now
        request.response().setStatusCode(404).end();
        return;
      }
      int pk = Integer.parseInt(pkParam);

      // Lookup the message from the database
      dbb.transactAsync(db -> {
        // Note this part happens on a worker thread
        metric.checkpoint("worker");
        String s = db.get().toSelect("select message from t where pk=?").argInteger(pk).queryStringOrEmpty();
        metric.checkpoint("db");
        return s;
      }, result -> {
        // Now we are back on the event loop thread
        metric.checkpoint("result");
        request.response().bodyEndHandler(h -> {
          metric.done("sent", request.response().bytesWritten());
          log.debug("Served request: " + metric.getMessage());
        });
        if (result.succeeded()) {
          request.response().setStatusCode(200).putHeader("content-type", "text/plain").end(result.result());
        } else {
          log.error("Returning 500 to client", result.cause());
          request.response().setStatusCode(500).end();
        }
      });
    }).listen(8123, result ->
        log.info("Started server. Go to http://localhost:8123/?pk=1 or http://localhost:8123/?pk=2")
    );

    // Attempt to do a clean shutdown on JVM exit
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      log.debug("Trying to stop the server nicely");
      try {
        synchronized (lock) {
          // First shutdown Vert.x
          vertx.close(h -> {
            log.debug("Vert.x stopped, now closing the connection pool");
            synchronized (lock) {
              // Then shutdown the database pool
              dbb.close();
              log.debug("Server stopped");
              lock.notify();
            }
          });
          lock.wait(30000);
        }
      } catch (Exception e) {
        e.printStackTrace();
      }
    }));
  }

  public static void main(final String[] args) {
    try {
      new VertxServer().run();
    } catch (Exception e) {
      e.printStackTrace();
      System.exit(1);
    }
  }
}
