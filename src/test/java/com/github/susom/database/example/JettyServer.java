package com.github.susom.database.example;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.AbstractHandler;
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
    server.setHandler(new AbstractHandler() {
      @Override
      public void handle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response)
          throws IOException, ServletException {
        Metric metric = new Metric(true);
        try {
          // Read the query parameter from the request
          String pkParam = request.getParameter("pk");
          if (pkParam == null) {
            // Probably a favicon or similar request we ignore for now
            response.setStatus(404);
            baseRequest.setHandled(true);
            return;
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
            response.setContentType("text/plain");
            response.setStatus(200);
            response.getOutputStream().print(s);
            baseRequest.setHandled(true);
          });
        } catch (Exception e) {
          log.error("Returning 500 to client", e);
          response.setStatus(500);
          baseRequest.setHandled(true);
        }
        log.debug("Served request: " + metric.getMessage());
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
