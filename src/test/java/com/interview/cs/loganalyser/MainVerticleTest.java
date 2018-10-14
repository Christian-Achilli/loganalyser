package com.interview.cs.loganalyser;

import io.vertx.core.DeploymentOptions;
import io.vertx.core.file.OpenOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.rxjava.core.Vertx;
import io.vertx.rxjava.core.file.AsyncFile;
import io.vertx.rxjava.core.parsetools.RecordParser;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.concurrent.atomic.AtomicInteger;

@RunWith(VertxUnitRunner.class)
public class MainVerticleTest {

  private Vertx vertx;

  @Before
  public void setUp(TestContext tc) {
    vertx = Vertx.vertx();
    DeploymentOptions options = new DeploymentOptions()
      .setConfig(new JsonObject().put("file.name", "src/test/resources/smallsample.log")
      );
    vertx.deployVerticle(MainVerticle.class.getName(), options, tc.asyncAssertSuccess());
  }

  @After
  public void tearDown(TestContext tc) {
    vertx.close(tc.asyncAssertSuccess());
  }

  AtomicInteger rowCounter = new AtomicInteger();

  @Test
  public void saved_to_db(TestContext tc) {
    Async async = tc.async();
    RecordParser assertParser = RecordParser.newDelimited("\n", line -> rowCounter.incrementAndGet());
    vertx.fileSystem().open("db/logmonitor",
      new OpenOptions().setRead(true).setWrite(false).setCreate(false),
      fileStream -> {
        AsyncFile dbFile = fileStream.result();
        dbFile.handler(assertParser).endHandler(end -> {
          tc.assertEquals(3, rowCounter.get());
          async.complete();
        });
      });
  }

}
