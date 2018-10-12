package com.interview.cs.loganalyser;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.file.AsyncFile;
import io.vertx.core.file.OpenOptions;
import io.vertx.core.parsetools.RecordParser;

public class MainVerticle extends AbstractVerticle {

  @Override
  public void start() {

    AsyncFile asyncFile = vertx.fileSystem().openBlocking("sample.log", new OpenOptions().setRead(true).setWrite(false).setCreate(false));
    RecordParser recordParser = RecordParser.newDelimited("\n", bufferedLine -> {
        System.out.println("bufferedLine = " + bufferedLine);
      });

      asyncFile.handler(recordParser)
          .endHandler(v -> {
            asyncFile.close();
            System.out.println("Done");
          });
  }



}
