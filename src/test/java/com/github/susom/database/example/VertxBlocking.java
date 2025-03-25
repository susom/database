package com.github.susom.database.example;

import static java.lang.Thread.sleep;

import com.github.susom.database.Config;
import com.github.susom.database.ConfigFrom;
import com.github.susom.database.DatabaseProviderVertx;
import com.github.susom.database.DatabaseProviderVertx.Builder;
import io.vertx.core.Vertx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Demo of asynchronous operations with Vertx and HyperSQL.
 */
public class VertxBlocking {
  private static final Logger log = LoggerFactory.getLogger(VertxBlocking.class);

  public void run() throws Exception {
    Vertx vertx = Vertx.vertx();
    Config config = ConfigFrom.firstOf()
        .value("database.url", "jdbc:hsqldb:file:target/hsqldb;shutdown=true")
        .value("database.pool.size", "2").get();
    Builder dbb = DatabaseProviderVertx.pooledBuilder(vertx, config)
        .withSqlInExceptionMessages()
        .withSqlParameterLogging();

    vertx.runOnContext(v -> {
      log.info("Running on the event loop thread");
      dbb.transactAsync(db -> {
        log.info("Sleeping for 5s");
        sleep(5000);
        log.info("Done sleeping");
        return db.get().toSelect("select 1 from information_schema.system_users limit 1").queryIntegerOrZero();
      }, ar -> {
        log.info("Completed query: " + ar.result());
      });

      for (int i = 2; i < 20; i++) {
        int q = i;
        dbb.transactAsync(db ->
          db.get().toSelect("select ? from information_schema.system_users limit 1").argInteger(q).queryIntegerOrZero()
        , ar -> {
          log.info("Completed query: " + ar.result());
        });
      }
    });
    sleep(6000);
    vertx.close(h -> {
      dbb.close();
    });
  }

  public static void main(final String[] args) {
    try {
      new VertxBlocking().run();
    } catch (Exception e) {
      e.printStackTrace();
      System.exit(1);
    }
  }
}
