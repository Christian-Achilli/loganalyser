package com.interview.cs.loganalyser;

import com.google.gson.Gson;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.file.AsyncFile;
import io.vertx.core.file.OpenOptions;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.parsetools.RecordParser;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.ext.sql.SQLConnection;

public class MainVerticle extends AbstractVerticle {

  @Override
  public void start() {

    AsyncFile asyncFile = vertx.fileSystem().openBlocking("sample.log", new OpenOptions().setRead(true).setWrite(false).setCreate(false));
    Gson gson = new Gson();
    JsonObject config = new JsonObject()
      .put("url", "jdbc:hsqldb:mem:test?shutdown=true")
      .put("driver_class", "org.hsqldb.jdbcDriver")
      .put("max_pool_size", 30);

    String insertRawLog = "insert into test values('%s','%s','%s','%s',%d)";

    final JDBCClient client = JDBCClient.createShared(vertx, config);

    client.getConnection(conn -> {

      if (conn.failed()) {
        System.err.println(conn.cause().getMessage());
        return;
      }

      final SQLConnection connection = conn.result();

      //drop
      connection.execute("drop table test", dropres -> {
        if (dropres.failed()) {
          System.err.println(dropres.cause());
        }
        //create
        connection.execute("create table test(id varchar(10), state varchar(8), type varchar(50), host varchar(20), timestamp bigint)", res -> {
          if (res.failed()) {
            throw new RuntimeException(res.cause());
          }

          RecordParser recordParser = RecordParser.newDelimited("\n", bufferedLine -> {

            System.out.println("bufferedLine = " + bufferedLine);
            // insert
            LogLine logLine = gson.fromJson(bufferedLine.toString(), LogLine.class);
            connection.execute(String.format(insertRawLog, logLine.id, logLine.state, logLine.type, logLine.host, logLine.timestamp), insertResult -> {
              if (insertResult.failed()) {
                System.err.println("Error inserting " + bufferedLine + ": " + insertResult.cause());
                return;
              }
            });
          });

          asyncFile.handler(recordParser).endHandler(v -> {
            asyncFile.close();
            System.out.println("Done");
            // and close the connection

            connection.query("select * from test", selectall -> {
              for (JsonArray line : selectall.result().getResults()) {
                System.out.println(line.encode());
              }
              connection.close(done -> {
                if (done.failed()) {
                  throw new RuntimeException(done.cause());
                }
              });
            });

          });
        });
      });


    });

  }

  class LogLine {
    String id;
    String state;
    String type;
    String host;
    long timestamp;

  }

}


