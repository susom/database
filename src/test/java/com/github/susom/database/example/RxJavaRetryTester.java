package com.github.susom.database.example;

import com.github.susom.database.*;
import com.github.susom.database.DatabaseProviderRx.Builder;
import io.reactivex.Observable;
import io.reactivex.Single;
import io.reactivex.schedulers.Schedulers;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/** Debug db-to-avro retry problems * */
public class RxJavaRetryTester {
  private static final Logger log = LoggerFactory.getLogger(RxJavaRetryTester.class);

  public static void main(final String[] args) {
    try {
      new RxJavaRetryTester().run();
    } catch (Exception e) {
      e.printStackTrace();
      System.exit(1);
    }
  }

  public void run() throws Exception {

    JsonObject jsonConfig =
        new JsonObject()
            .put("database.url", "jdbc:hsqldb:file:target/hsqldb;shutdown=true")
            .put("database.user", "SA")
            .put("database.password", "")
            .put("database.pool.size", 20);

    Config config = ConfigFrom.firstOf().custom(jsonConfig::getString).get();
    Builder dbb =
        DatabaseProviderRx.pooledBuilder(config)
            .withSqlInExceptionMessages()
            .withSqlParameterLogging();

    dbb.transact(
        db -> {
          db.get().dropTableQuietly("t");
          new Schema()
              .addTable("t")
              .addColumn("pk")
              .primaryKey()
              .table()
              .addColumn("message")
              .asString(80)
              .schema()
              .execute(db);

          db.get()
              .toInsert("insert into t (pk,message) values (?,?)")
              .argInteger(1)
              .argString("Hello World!")
              .batch()
              .argInteger(2)
              .argString("Goodbye!")
              .insertBatch();
        });

    List<String> sqlQueries = new ArrayList<>();
    for (int i = 1; i <= 30; i++) {
      if (i == 20) {
        sqlQueries.add("SELECTXXXX;");
      } else {
        sqlQueries.add("select message from t where pk=" + i + ";");
      }
    }

    ExecutorService threadPool = Executors.newFixedThreadPool(5);

    Observable.fromIterable(sqlQueries)
        .flatMap(
            sql ->
                    Single.defer(() -> // remove this line to show buggy behavior
                            dbb.transactRx(
                                db -> {
                                  if (sql.contains("SELECTXXXX")) {
                                    log.error(">>>>>>>>>>> RUNNING BAD SQL <<<<<<<<<<<<");
                                    log.error(
                                        ">>>>>>>>>>> DB hash: {}",
                                        db
                                            .hashCode()); // same object when retried, other
                                                          // statements have
                                    // a new one each time
                                  }
                                  db.get()
                                      .toSelect(sql)
                                      .queryMany(row -> row.getStringOrNull("message"));
                                  return sql;
                                })
                    ) // remove this line to show buggy behavior
                    .toObservable()
                    .retryWhen(
                        errors -> {
                          Observable<Integer> range = Observable.range(1, 3);
                          Observable<Observable<Long>> zipWith =
                              errors.zipWith(
                                  range,
                                  (e, i) ->
                                      i < 3
                                          ? Observable.timer(
                                              i, TimeUnit.SECONDS, Schedulers.from(threadPool))
                                          : Observable.error(e));
                          return Observable.merge(zipWith);
                        })
                    .subscribeOn(Schedulers.from(threadPool)))
        .blockingSubscribe(
            results -> log.warn("{} completed", results),
            error -> {
              log.error("Final error: {}", error.getMessage());
            });

    log.warn("### SHUTTING DOWN THREAD POOL ###");
    threadPool.shutdown();

    log.info("All done!");
    Thread.sleep(2000);

    // Attempt to do a clean shutdown on JVM exit
    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  log.debug("Trying to stop the server nicely");
                  try {
                    dbb.close();
                  } catch (Exception e) {
                    e.printStackTrace();
                  }
                }));
  }
}
