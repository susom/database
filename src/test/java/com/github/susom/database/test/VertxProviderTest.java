package com.github.susom.database.test;

import static java.lang.Thread.sleep;
import static org.junit.Assert.assertEquals;

import com.github.susom.database.Config;
import com.github.susom.database.ConfigFrom;
import com.github.susom.database.DatabaseProviderVertx;
import com.github.susom.database.DatabaseProviderVertx.Builder;
import io.vertx.core.Vertx;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Verify some asynchronous/blocking behavior of queries in the VertxProvider.
 */
@RunWith(VertxUnitRunner.class)
public class VertxProviderTest {
  private final Logger log = LoggerFactory.getLogger(VertxProviderTest.class);

  static {
    // We will put all Derby related files inside ./build to keep our working copy clean
    File directory = new File("target").getAbsoluteFile();
    if (directory.exists() || directory.mkdirs()) {
      System.setProperty("derby.stream.error.file", new File(directory, "derby.log").getAbsolutePath());
    }
  }

  @Test
  public void testSlowOperationBlocking(TestContext context) {
    Async async = context.async();

    Vertx vertx = Vertx.vertx();

    // Set pool size to 1 so we can test blocking behavior: the
    // first request will block, and the rest will queue up
    Config config = ConfigFrom.firstOf()
        .value("database.url", "jdbc:derby:target/testdb;create=true")
        .value("database.pool.size", "1").get();
    Builder dbb = DatabaseProviderVertx.pooledBuilder(vertx, config);

    List<Integer> results = runQueries(vertx, dbb, async);

    async.awaitSuccess();
    assertEquals(List.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19), results);
    vertx.close();
    dbb.close();
  }

  @Test
  public void testSlowOperationNonBlocking(TestContext context) {
    Async async = context.async();

    Vertx vertx = Vertx.vertx();

    // Set pool size to 2 so we can test blocking behavior: the
    // first request will block, and the rest will execute in order
    // while it is blocked
    Config config = ConfigFrom.firstOf()
        .value("database.url", "jdbc:derby:target/testdb;create=true")
        .value("database.pool.size", "2").get();
    Builder dbb = DatabaseProviderVertx.pooledBuilder(vertx, config);

    List<Integer> results = runQueries(vertx, dbb, async);

    async.awaitSuccess();
    assertEquals(List.of(2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 1), results);
    vertx.close();
    dbb.close();
  }

  private List<Integer> runQueries(Vertx vertx, Builder dbb, Async async) {
    List<Integer> results = new ArrayList<>();
    vertx.runOnContext(v -> {
      log.info("Running on the event loop thread");
      dbb.transactAsync(db -> {
        log.info("Sleeping for 5s");
        sleep(5000);
        log.info("Done sleeping");
        return db.get().toSelect("values (1)").queryIntegerOrZero();
      }, ar -> {
        log.info("Completed query: " + ar.result());
        results.add(ar.result());
        if (results.size() == 19) {
          async.complete();
        }
      });

      for (int i = 2; i < 20; i++) {
        int q = i;
        dbb.transactAsync(db ->
                db.get().toSelect("values (" + q + ")").queryIntegerOrZero()
            , ar -> {
              log.info("Completed query: " + ar.result());
              results.add(ar.result());
              if (results.size() == 19) {
                async.complete();
              }
            });
      }
    });
    return results;
  }
}
