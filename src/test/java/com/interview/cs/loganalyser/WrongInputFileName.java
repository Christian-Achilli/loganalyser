package com.interview.cs.loganalyser;

import io.vertx.core.DeploymentOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.rxjava.core.Vertx;
import org.apache.log4j.LogManager;
import org.apache.log4j.spi.Filter;
import org.apache.log4j.spi.LoggingEvent;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.List;

/**
 * Use case: the file name specified does not exists
 */
@RunWith(VertxUnitRunner.class)
public class WrongInputFileName {

  public static final int DELAY = 1000; // this is to allow all log messages to come through
  private Vertx vertx;
  private List<LoggingEvent> stdoutLogEvents;

  @Before
  public void setUp(TestContext tc) throws Exception {
    stdoutLogEvents = new ArrayList<>();
    vertx = Vertx.vertx();
    LogManager.getRootLogger().getAppender("stdout").addFilter(new Filter() {
      @Override
      public int decide(LoggingEvent event) {
        stdoutLogEvents.add(event);
        return 0;
      }
    });
    DeploymentOptions options = new DeploymentOptions()
      .setConfig(new JsonObject().put("file.name", "src/main/resources/not_existent.log")
      );
    vertx.deployVerticle(MainVerticle.class.getName(), options, tc.asyncAssertFailure());
  }

  @After
  public void tearDown(TestContext tc) {
    vertx.close(tc.asyncAssertSuccess());
  }

  @Test
  public void verify_log_statements(TestContext tc) {
    vertx.setTimer(DELAY, t -> {
      Async async = tc.async();
      tc.assertTrue(hasWrongFileNameMessage());
      async.complete();
    });
  }

  private boolean hasWrongFileNameMessage() {
    return stdoutLogEvents.stream().anyMatch(log -> log.getMessage().equals("Could not open input file: src/main/resources/not_existent.log"));
  }


}
