import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.susom.database.Config;
import com.github.susom.database.ConfigFrom;
import com.github.susom.database.DatabaseProviderVertx;
import com.github.susom.database.DatabaseProviderVertx.Builder;
import com.github.susom.database.Metric;
import com.github.susom.database.Schema;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;

/**
 * <p>Demo of using some com.github.susom.database classes with Vertx and HyperSQL.
 * In this version there are two database pools, one for fast queries and one
 * for slow queries. The idea is that saturating the slow pool won't affect
 * server calls using the fast pool, and saturating both of the pools won't
 * affect server calls that do not use the database.</p>
 *
 * <p>To test this, you can fire up the server and then run commands like the
 * following in parallel in three separate terminals:</p>
 *
 * <code>
 *   ab -k -c 50 -n 100 http://localhost:8123/slow
 *   ab -k -c 500 -n 10000 http://localhost:8123/fast
 *   ab -k -c 500 -n 10000 http://localhost:8123/static
 * </code>
 */
public class VertxServerFastAndSlow {
  private static final Logger log = LoggerFactory.getLogger(VertxServerFastAndSlow.class);
  private final Object lock = new Object();

  public void run() throws Exception {
    Config config = ConfigFrom.firstOf().value("database.url", "jdbc:hsqldb:file:target/hsqldb;shutdown=true").get();
    Vertx vertx = Vertx.vertx();
    Builder fastDb = DatabaseProviderVertx.pooledBuilder(vertx, config);
    Builder slowDb = DatabaseProviderVertx.pooledBuilder(vertx, config);

    // Set up a table with some data we can query later
    fastDb.transact(db -> {
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
      Handler<AsyncResult<String>> sendResponse = result -> {
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
      };

      switch (request.path()) {
      case "/fast":
        fastDb.transactAsync(db -> {
          metric.checkpoint("worker");
          String s = db.get().toSelect("select message from t where pk=?").argInteger(1).queryStringOrEmpty();
          metric.checkpoint("db");
          return s;
        }, sendResponse);
        break;
      case "/slow":
        slowDb.transactAsync(db -> {
          metric.checkpoint("worker");
          String s = db.get().toSelect("select message from t where pk=?").argInteger(2).queryStringOrEmpty();
          metric.checkpoint("db");
          // Simulate slow query
          Thread.sleep(2000);
          metric.checkpoint("sleep");
          return s;
        }, sendResponse);
        break;
      default:
        sendResponse.handle(Future.succeededFuture("Hi"));
      }
    }).listen(8123, result ->
        log.info("Started server. Go to one of:\n    http://localhost:8123/slow\n    http://localhost:8123/fast"
            + "\n    http://localhost:8123/static")
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
              // Then shutdown the database pools
              fastDb.close();
              slowDb.close();
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
      new VertxServerFastAndSlow().run();
    } catch (Exception e) {
      e.printStackTrace();
      System.exit(1);
    }
  }
}
