package com.interview.cs.loganalyser;


import com.google.gson.Gson;
import io.netty.util.internal.StringUtil;
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
import io.vertx.rxjava.ext.jdbc.JDBCClient;
import io.vertx.rxjava.ext.sql.SQLConnection;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.Map;
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
    InternalLoggerFactory.setDefaultFactory(Log4JLoggerFactory.INSTANCE); // this is needed to fix a log quirk in netty
    //input parameters


//TODO if you don't delete the previous run the data will be appended to the current DB
  }


  @Override
  public void start(Future<Void> fut) {

    LOG.info("************* HOW TO USE THE LOG ANALYSER *******************");
    LOG.info("The file to be analysed has to be specified in a json config file.");
    LOG.info("The json config file content is in the format:");
    LOG.info("{");
    LOG.info("\t\"file.name\":\"<absolute file path>\"");
    LOG.info("}");
    LOG.info("Invoke command: 'java -jar loganalyzer-vertx-1.0-SNAPSHOT-fat.jar -conf <path to conf file>");
    LOG.info("************************************************************\n");


    fileName = config().getString("file.name");
    LOG.info("File to be analysed: " + fileName);

    if (vertx.fileSystem().existsBlocking("db/")) {
      vertx.fileSystem().deleteRecursiveBlocking("db/", true);
      LOG.info("Data from the previous run has been deleted");
    }


    long started = System.currentTimeMillis();


    client = JDBCClient.createShared(vertx, config);
    HashMap<String, String> tempMap = new HashMap<>();//vertx.sharedData().getLocalMap("log-ids");

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
    });// end get connection
  }

  private void logLineProcessor(Map<String, String> tempMap, Buffer rawLogLine, Handler<Buffer> bufferHandler) {

    if (analyzedLogLines.incrementAndGet() % 10000 == 0) {
      System.out.printf("\rSo far analyzed: " + analyzedLogLines.intValue() + " inserted: " + insertedRecords.intValue());
    }

    LogLine logLine = gson.fromJson(rawLogLine.toJsonObject().toString(), LogLine.class);

    if(StringUtil.isNullOrEmpty(logLine.id) || StringUtil.isNullOrEmpty(logLine.state) || logLine.timestamp == 0) {
      LOG.warn("Malformed transaction log in '"+fileName+"' at line " + analyzedLogLines.intValue());
      return;
    }

    LogLine logInMap = gson.fromJson(tempMap.get(logLine.id), LogLine.class);
    if (null == logInMap) {
      tempMap.put(logLine.id, rawLogLine.toJsonObject().toString());
    } else {
      if(logInMap.state.equals(logLine.state)) {
        LOG.warn("Transaction '" + logInMap.id + "' state '" + logInMap.state + "' has been logged at timestamp " + logInMap.timestamp + " and " + logLine.timestamp);
        return;
      }
      tempMap.remove(logLine.id);
      //TODO handle error case when both state are the same (wrong log statement in the code)
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

  private void closeDown(long started, Map<String, String> tempMap, SQLConnection connection) {
    LOG.info("Done inserting to DB");
    LOG.debug("TempMap size: " + tempMap.size());
    if(tempMap.size() != 0) {
      LOG.warn("The following log transaction miss either STARTED or FINISHED:");
      tempMap.values().stream().forEach(r -> LOG.warn(r.toString()));
    }

    connection.close(done -> {
      LOG.debug("DB Connection closed.");
      LOG.info("Completed in :" + (System.currentTimeMillis() - started));
      LOG.info("Total rows inserted to DB: " + insertedRecords.getAndIncrement());
      LOG.info("Total rows analyzed in log file: " + analyzedLogLines.getAndIncrement());
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
        LOG.debug("Inserted: " + jsonArray.toJsonArray());
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
    String state;
    String type;
    String host;
    long timestamp;

  }

}


