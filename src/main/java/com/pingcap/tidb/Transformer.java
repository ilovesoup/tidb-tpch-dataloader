package com.pingcap.tidb;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Scanner;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class Transformer {
  public static final String READ_FILE_NAME =
//      "/home/novemser/Downloads/tidb/tidb-enterprise-tools-latest-linux-amd64/bin/export-20180112-172913/tispark_test.tb_downgrade.sql";
      "/home/novemser/Documents/Code/Java/tidb-tpch-dataloader/tpch-dbgen/part.tbl";
  public static final String WRITE_FILE_NAME =
      "/home/novemser/part.sql";
  private AtomicInteger readCnt = new AtomicInteger(0);
  private int BATCH_NUM = 10000;
  private BlockingQueue<String[]> readBuffer = new LinkedBlockingQueue<>(BATCH_NUM);
  private ExecutorService pool = Executors.newFixedThreadPool(2);
  private boolean endOfRead = false;

  public static void main(String[] args) throws Exception {
    Transformer transformer = new Transformer();
    transformer.loadNIO();
  }

  private void load() throws FileNotFoundException {
    try (Scanner sc = new Scanner(new File(READ_FILE_NAME))) {
      int lineNum = 0;
      while (sc.hasNextLine()) {
        String line = sc.nextLine();
        lineNum++;
        if (lineNum % 100000 == 0) {
          System.out.println(lineNum);
        }
      }
    }
  }

  private void loadNIO() {
    startReader();
    startWriter();
    close();
  }

  private void close() {
    pool.shutdown();
  }

  private void startReader() {
    pool.submit(() -> {
      Path path = Paths.get(READ_FILE_NAME);
      try (BufferedReader reader = Files.newBufferedReader(path)) {
        String currentLine;
        long start = System.currentTimeMillis();
        while ((currentLine = reader.readLine()) != null) {
          readCnt.addAndGet(1);
          String[] result = currentLine.split("\\|");
          readBuffer.put(result);
        }
        System.out.println("Finished reading " + readCnt + " lines using time(ms):" + (System.currentTimeMillis() - start));
      } catch (Exception e) {
        e.printStackTrace();
      } finally {
        endOfRead = true;
      }
    });
  }

  private void startWriter() {
    pool.submit(() -> {
      Path path = Paths.get(WRITE_FILE_NAME);
      try (BufferedWriter writer = Files.newBufferedWriter(path)) {
        long start = System.currentTimeMillis();
        int writeCnt = 0;
        writer.write("/*!40101 SET NAMES binary*/;\n" +
            "/*!40014 SET FOREIGN_KEY_CHECKS=0*/;\n" +
            "/*!40103 SET TIME_ZONE='+00:00' */;\n");
        while (true) {
          if (readBuffer.isEmpty() && endOfRead) {
            break;
          }

          StringBuilder builder = new StringBuilder();
          for (int i = 0; i < BATCH_NUM; i++) {
            if (i == 0) {
              writer.write("INSERT INTO `PART` VALUES\n");
            }
            // append insert values
            builder.append("(\"")
                .append(String.join("\",\"", readBuffer.take()))
                .append("\")");
            writeCnt++;
            if (i == BATCH_NUM - 1 ||
                (endOfRead && readBuffer.isEmpty())) {
              builder.append(";");
              break;
            } else {
              builder.append(",");
            }
          }
          if (builder.length() <= 0) {
            continue;
          }
          writer.write(builder.toString());
          writer.newLine();
          writer.flush();
        }
        writer.flush();
        System.out.println("Finished writing " + writeCnt + " lines using time(ms):" + (System.currentTimeMillis() - start));
      } catch (Exception e) {
        e.printStackTrace();
      }

    });
  }
}
