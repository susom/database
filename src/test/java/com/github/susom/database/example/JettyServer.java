package com.github.susom.database.example;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Response;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.util.Callback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.susom.database.Config;
import com.github.susom.database.DatabaseProvider;
import com.github.susom.database.DatabaseProvider.Builder;
import com.github.susom.database.Metric;
import com.github.susom.database.Schema;

/**
 * Demo of using some com.github.susom.database classes with Jetty and HyperSQL.
 */
public class JettyServer {
  private static final Logger log = LoggerFactory.getLogger(JettyServer.class);

  public void run() throws Exception {
    // Set up the database pool
    Config config = Config.from().value("database.url", "jdbc:hsqldb:file:target/hsqldb;shutdown=true").get();
    Builder dbb = DatabaseProvider.pooledBuilder(config)
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
    Server server = new Server(8080);
    server.setHandler(new Handler.Abstract() {
      @Override
      public boolean handle(Request request, Response response, Callback callback) throws Exception {
        Metric metric = new Metric(true);
        try {
          // Read the query parameter from the request
          String pkParam = Request.getParameters(request).getValue("pk");
          if (pkParam == null) {
            // Probably a favicon or similar request we ignore for now
            Response.writeError(request, response, callback, 404);
            return true;
          }
          int pk = Integer.parseInt(pkParam);

          // Lookup the message from the database
          dbb.transact(db -> {
            // Note this part happens on a worker thread
            metric.checkpoint("worker");
            String s = db.get().toSelect("select message from t where pk=?").argInteger(pk).queryStringOrEmpty();
            metric.checkpoint("db");
            metric.checkpoint("result");
            metric.done("sent", s.length());
            
            response.getHeaders().put("Content-Type", "text/plain");
            response.setStatus(200);
            try {
              ByteBuffer buffer = ByteBuffer.wrap(s.getBytes(StandardCharsets.UTF_8));
              response.write(true, buffer, callback);
            } catch (Exception e) {
              callback.failed(e);
            }
          });
        } catch (Exception e) {
          log.error("Returning 500 to client", e);
          Response.writeError(request, response, callback, 500);
        }
        log.debug("Served request: " + metric.getMessage());
        return true;
      }
    });

    server.start();
    log.info("Started server. Go to http://localhost:8080/?pk=1 or http://localhost:8080/?pk=2");

    // Attempt to do a clean shutdown on JVM exit
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      log.debug("Trying to stop the server nicely");
      try {
        // First shutdown Vert.x
        server.stop();
        log.debug("Jetty stopped, now closing the connection pool");
        dbb.close();
        log.debug("Connection pool closed");
      } catch (Exception e) {
        e.printStackTrace();
      }
    }));
    server.join();
  }

  public static void main(final String[] args) {
    try {
      new JettyServer().run();
    } catch (Exception e) {
      e.printStackTrace();
      System.exit(1);
    }
  }
}
