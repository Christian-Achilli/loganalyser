package com.interview.cs.loganalyser;


import com.google.gson.Gson;
import io.netty.util.internal.logging.InternalLoggerFactory;
import io.netty.util.internal.logging.Log4JLoggerFactory;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.file.OpenOptions;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.rxjava.core.AbstractVerticle;
import io.vertx.rxjava.core.buffer.Buffer;
import io.vertx.rxjava.core.file.AsyncFile;
import io.vertx.rxjava.core.parsetools.RecordParser;
import io.vertx.rxjava.core.shareddata.LocalMap;
import io.vertx.rxjava.ext.jdbc.JDBCClient;
import io.vertx.rxjava.ext.sql.SQLConnection;
import org.apache.log4j.Logger;

import java.util.concurrent.atomic.AtomicInteger;

import static io.vertx.rxjava.core.parsetools.RecordParser.newDelimited;

public class MainVerticle extends AbstractVerticle {

  private static final Logger LOG = Logger.getLogger(MainVerticle.class);

  private long threshold = 4;
  private JsonObject config = new JsonObject()
    .put("url", "jdbc:hsqldb:file:db/TESTDB?shutdown=true")
    .put("driver_class", "org.hsqldb.jdbcDriver")
    .put("max_pool_size", 50)
    .put("auto_commit_on_close", true);
  Gson gson = new Gson();
  private static final String TABLE_NAME = "LOG_MONITOR";
  private static final String INSERT_RAW_LOG = "INSERT INTO " + TABLE_NAME + " VALUES(?,?,?,?,?)";
  private static final String CREATE_TABLE = "CREATE TEXT TABLE IF NOT EXISTS " + TABLE_NAME + "( id varchar(10), duration bigint, type varchar(50), host varchar(20), alert varchar(10))";
  private static final String SET_TABLE = "SET TABLE " + TABLE_NAME + " SOURCE \"logmonitor;ignore_first=false;all_quoted=true;cache_rows=10000;cache_size=1000\"";
  private final AtomicInteger insertedRecords = new AtomicInteger();
  private final AtomicInteger analyzedLogLines = new AtomicInteger();
  private JDBCClient client;

  private String fileName;

  public MainVerticle() {
    InternalLoggerFactory.setDefaultFactory(Log4JLoggerFactory.INSTANCE);
    //input parameters



/*    System.out.println("Hello and welcome. \n Please execute this application from the folder where the log file is.");
    System.out.println("Please insert the name of the file:");
    Scanner scanner = new Scanner(System.in);
    fileName = scanner.nextLine();
    System.out.println("Your file name is " + fileName);*/

    //
//TODO if you don't delete the previous run the data will be appended to the current DB
  }


  @Override
  public void start(Future<Void> fut) {

    fileName = "/Users/chrisachilli/creditsuisse/bigsample.log";//config().getString("file.name");
    System.out.println("------->>>>>>> " + fileName);

    if (vertx.fileSystem().existsBlocking("db/")) {
      vertx.fileSystem().deleteRecursiveBlocking("db/", true);
    }


    long started = System.currentTimeMillis();


    client = JDBCClient.createShared(vertx, config);
    LocalMap<String, String> tempMap = vertx.sharedData().getLocalMap("log-ids");

    RecordParser recordParser = newDelimited("\n",
      processedLogLine -> logLineProcessor(tempMap, processedLogLine,
        insertParameters ->

          singleSaveBuffer(insertParameters,
            Future.future(r -> insertedRecords.incrementAndGet()))));


    client.getConnection(conn -> {
      if (conn.failed()) {
        LOG.error("FATAL: could not obtain db connection: " + conn.cause().getMessage(), conn.cause());
        return;
      }
      final SQLConnection connection = conn.result();
      //drop
      connection.execute("TRUNCATE TABLE " + TABLE_NAME, truncate -> {
        if (truncate.failed()) {
          System.err.println(truncate.cause());
        }
        connection.execute("COMMIT", commit -> {
          //create
          connection.execute(CREATE_TABLE, res -> {
            if (res.failed()) {
              connection.close();
              throw new RuntimeException(res.cause());
            } else {
              connection.execute(SET_TABLE, settable -> {

                vertx.fileSystem().open(fileName, new OpenOptions().setRead(true).setWrite(false).setCreate(false), inputFileIsOpen -> {

                  AsyncFile asyncFile = inputFileIsOpen.result();

                  asyncFile
                    .handler(recordParser)
                    .endHandler(v -> {
                      asyncFile.close();
                      closeDown(started, tempMap, connection);
                      fut.complete();
                    });///end async file handler
                });

              });// end SET table
            }// else
          });// end create table
        }); //end commit
      });// end drop table
    });// end get connection
  }

  private void logLineProcessor(LocalMap<String, String> tempMap, Buffer rawLogLine, Handler<Buffer> bufferHandler) {

    if (analyzedLogLines.incrementAndGet() % 10000 == 0) {
      //LOG.info("So far analyzed: " +analyzedLogLines.intValue());
      System.out.printf("\rSo far analyzed: " + analyzedLogLines.intValue() + " inserted: " + insertedRecords.intValue());
    }

    LogLine logLine = gson.fromJson(rawLogLine.toJsonObject().toString(), LogLine.class);
    LogLine logInMap = gson.fromJson(tempMap.get(logLine.id), LogLine.class);
    if (null == logInMap) {
      tempMap.put(logLine.id, rawLogLine.toJsonObject().toString());
    } else {
      tempMap.remove(logLine.id);
      //TODO handle error case when both state are the same (wrong log statement in the code)
      //TODO handle error case when transaction id are not unique
      long delta = Math.abs(logInMap.timestamp - logLine.timestamp);
      String alert = "false";
      if (delta >= threshold) {
        alert = "true";
      }

      JsonArray params = new JsonArray()
        .add(logLine.id)
        .add(delta)
        .add(logLine.type == null ? "" : logLine.type)
        .add(logLine.host == null ? "" : logLine.host)
        .add(alert);

      bufferHandler.handle(new Buffer(params.toBuffer()));
    }
  }

  private void closeDown(long started, LocalMap<String, String> tempMap, SQLConnection connection) {
    LOG.info("Done inserting to DB");
    LOG.debug("TempMap size /1: " + tempMap.size());

    connection.close(done -> {
      LOG.debug("DB Connection closed.");
      LOG.debug("TempMap size /2: " + tempMap.size());
      LOG.info("Completed in :" + (System.currentTimeMillis() - started));
      LOG.info("Total rows to DB: " + insertedRecords.getAndIncrement());
      LOG.info("Total rows analyzed: " + analyzedLogLines.getAndIncrement());
      if (done.failed()) {
        LOG.error("Exception while closing the DB connection during the closeDown procedure", done.cause());
        throw new RuntimeException(done.cause());
      }

    });
  }

  private void singleSaveBuffer(Buffer jsonArray, Future complete) {

    client.getConnection(connection -> {
      SQLConnection singleConnection = connection.result();
      singleConnection.queryWithParams(INSERT_RAW_LOG, jsonArray.toJsonArray(), insertResult -> {
        if (insertResult.failed()) {
          LOG.warn("WARNING: Error inserting " + jsonArray.toJsonArray() + ": " + insertResult.cause().getMessage(), insertResult.cause());
          return;
        }
        LOG.debug("Saved: " + jsonArray.toJsonArray());
        singleConnection.close();
        complete.complete();
      });
    });
  }

  /**
   * DTO for the lines of the file to be analysed
   */
  private class LogLine {
    String id;
    String type;
    String host;
    long timestamp;

  }

}


