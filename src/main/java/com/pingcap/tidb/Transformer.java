package com.pingcap.tidb;

import org.apache.commons.cli.*;
import org.apache.commons.io.FileUtils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class Transformer {

  static class Context {
    enum Status {
      INITIAL,
      READING,
      FINISHED
    }

    private final String inputFileName;
    private String currentWriteFile;
    private AtomicInteger writeFileIdx;
    private final long MAX_BYTES_PER_FILE;
    private volatile AtomicLong bytesWrite;
    private volatile BlockingQueue<String[]> dataQueue;
    private volatile Status status;

    Context(String inputFileName, long max_bytes_per_file, BlockingQueue<String[]> queue, Status status) {
      this.inputFileName = inputFileName;
      MAX_BYTES_PER_FILE = max_bytes_per_file;
      this.dataQueue = queue;
      this.status = status;
      writeFileIdx = new AtomicInteger(1);
      bytesWrite = new AtomicLong(0);
    }

    void setCurrentWriteFile(String currentWriteFile) {
      this.currentWriteFile = currentWriteFile;
    }

    void putData(String[] data) throws InterruptedException {
      dataQueue.put(data);
    }

    BlockingQueue<String[]> getDataQueue() {
      return dataQueue;
    }

    Status getStatus() {
      return status;
    }

    void setStatus(Status status) {
      this.status = status;
    }

    String getDBName() {
      return "tpch";
    }

    String getTableName() {
      return inputFileName.replace(".tbl", "").toUpperCase();
    }

    String nextFileName() {
      return String.format("%s.%s.%09d.sql",
          getDBName(),
          inputFileName.replace(".tbl", "").toUpperCase(),
          writeFileIdx.getAndIncrement());
    }

    boolean isEmpty() {
      return dataQueue.isEmpty() && status == Status.FINISHED;
    }

    boolean needProceedNextFile() {
      boolean needProceed = bytesWrite.get() >= MAX_BYTES_PER_FILE;
      if (needProceed) {
        bytesWrite.set(0);
      }
      return needProceed;
    }

    void incBytesWrite(long val) {
      bytesWrite.addAndGet(val);
    }
  }

  private String TPCH_DIR = "";
  private String OUTPUT_DIR = "";
  private int MAX_ROWS_COUNT = 10000;
  private long MAX_BYTES_PER_FILE = 100 * 1024 * 1024; // Default to 100MB

  private Collection<File> sources;
  private Map<String, Context> contextMap = new ConcurrentHashMap<>();

  private ExecutorService readers;
  private ExecutorService writers;

  private Options options = new Options();
  private Date startTime;

  public static void main(String[] args) throws ParseException {
    Transformer transformer = new Transformer();
    CommandLineParser parser = new DefaultParser();
    CommandLine cmd = parser.parse(transformer.getOptions(), args);
    if (cmd.hasOption("help") || args.length <= 0) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp("java -jar tidb-tpch-dataloader-1.0-SNAPSHOT.jar [option]<arg>\nTPCH data transformer - CSV format to mydumper SQL files.\n", transformer.getOptions());
      System.exit(0);
    }
    if (cmd.hasOption("chunkFileSize")) {
      transformer.MAX_BYTES_PER_FILE = Long.parseLong(cmd.getOptionValue("chunkFileSize"));
    }
    if (cmd.hasOption("outputDir")) {
      transformer.OUTPUT_DIR = cmd.getOptionValue("outputDir");
      System.out.println("Output dir:\t" + transformer.OUTPUT_DIR);
    } else {
      System.out.println("You must specify -outputDir.");
      System.exit(0);
    }
    if (cmd.hasOption("tpchDir")) {
      transformer.TPCH_DIR = cmd.getOptionValue("tpchDir");
      System.out.println("TPCH data dir:\t" + transformer.TPCH_DIR);
    } else {
      System.out.println("You must specify -tpchDir.");
      System.exit(0);
    }
    if (cmd.hasOption("rowCount")) {
      transformer.MAX_ROWS_COUNT = Integer.parseInt(cmd.getOptionValue("rowCount"));
    }
    int readers = 2;
    int writers = 2;
    if (cmd.hasOption("readers")) {
      readers = Integer.parseInt(cmd.getOptionValue("readers"));
    }
    if (cmd.hasOption("writers")) {
      writers = Integer.parseInt(cmd.getOptionValue("writers"));
    }

    transformer.run(readers, writers);
  }

  private Transformer() {
    initOptions();
  }

  public void run(int reader, int writer) {
    prepareDataFiles();
    verifyData();
    initReaderWriter(reader, writer);
    transferToSQLFiles();
  }

  private void initOptions() {
    options.addOption("help", "Print this help");
    options.addOption("tpchDir", true, "Directory where you place your tpch data file in.");
    options.addOption("outputDir", true, "Directory where the transformed sql files will be placed in.");
    options.addOption("rowCount", true, "How many rows per `INSERT INTO` statement.");
    options.addOption("chunkFileSize", true, "Split tables into chunks of this output file size. This value is in MB.");
    options.addOption("readers", true, "Reader thread count.(one thread per file)");
    options.addOption("writers", true, "Writer thread count.(one thread per file)");
  }

  public Options getOptions() {
    return options;
  }

  private void initReaderWriter(int reader, int writer) {
    readers = Executors.newFixedThreadPool(reader);
    writers = Executors.newFixedThreadPool(writer);
  }

  private void prepareDataFiles() {
    sources = FileUtils.listFiles(new File(TPCH_DIR), new String[]{"tbl"}, true);
  }

  private void verifyData() {
    if (sources == null || sources.size() != 8) {
      throw new IllegalStateException("Data has not been loaded properly");
    } else {
      sources.stream().map(File::getName).forEach(System.out::println);
      System.out.println("Successfully detected " + sources.size() + " files.");
      System.out.println("------------------------------------------------");

      sources.stream().map(File::getName)
          .forEach(name -> contextMap
              .putIfAbsent(
                  name,
                  new Context(
                      name,
                      MAX_BYTES_PER_FILE,
                      new LinkedBlockingQueue<>(Math.min(MAX_ROWS_COUNT, 100000)),
                      Context.Status.INITIAL
                  )
              ));
    }
  }

  private void transferToSQLFiles() {
    startTime = new Date();
    startReader();
    startWriter();
    close();
  }

  private void close() {
    readers.shutdown();
    writers.shutdown();
    try {
      while (!readers.awaitTermination(1, TimeUnit.SECONDS) ||
              !writers.awaitTermination(1, TimeUnit.SECONDS)) {}
        writeMetaData();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private void writeMetaData() {
    try(BufferedWriter writer = Files.newBufferedWriter(Paths.get(OUTPUT_DIR + "metadata"))) {
      SimpleDateFormat format = new SimpleDateFormat("YYYY-MM-dd HH:mm:ss");
      String startStr = "Started dump at: " + format.format(startTime);
      String endStr = "Finished dump at: " + format.format(new Date());
      System.out.println("Writing metadata:");
      System.out.println(startStr);
      System.out.println(endStr);
      writer.write(startStr);
      writer.newLine();
      writer.write(endStr);
      writer.newLine();
      writer.flush();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private void startReader() {
    sources.forEach(file -> {
      String absPath = file.getAbsolutePath();
      String fileName = file.getName();
      Path path = Paths.get(absPath);
      readers.submit(() -> {
        int readCnt = 0;
        Context ctx = contextMap.get(fileName);
        Objects.requireNonNull(ctx);
        try (BufferedReader reader = Files.newBufferedReader(path)) {
          ctx.setStatus(Context.Status.READING);
          String currentLine;
          long start = System.currentTimeMillis();
          while ((currentLine = reader.readLine()) != null) {
            readCnt++;
            String[] result = currentLine.split("\\|");
            ctx.putData(result);
          }
          System.out.println("Finished reading " + readCnt +
              " lines from " + fileName + " using time(ms):" + (System.currentTimeMillis() - start));
        } catch (Exception e) {
          e.printStackTrace();
        } finally {
          ctx.setStatus(Context.Status.FINISHED);
        }
      });
    });
  }

  private void startWriter() {
    sources.stream()
        .map(File::getName)
        .forEach(fileName -> writers.submit(() -> {
          Context ctx = contextMap.get(fileName);
          while (!ctx.isEmpty()) {
            try (BufferedWriter writer = getFileBufferedWriter(ctx)) {
              writer.write("/*!40101 SET NAMES binary*/;\n" +
                  "/*!40014 SET FOREIGN_KEY_CHECKS=0*/;\n" +
                  "/*!40103 SET TIME_ZONE='+00:00' */;\n");

              while (true) {
                if (ctx.isEmpty() || ctx.needProceedNextFile()) {
                  break;
                }

                StringBuilder builder = new StringBuilder();
                for (int i = 0; i < MAX_ROWS_COUNT; i++) {
                  if (i == 0) {
                    writer.write("INSERT INTO `" + ctx.getTableName() + "` VALUES\n");
                  }
                  // append insert values
                  builder.append("(\"")
                      .append(String.join("\",\"", ctx.getDataQueue().take()))
                      .append("\")");

                  if (i == MAX_ROWS_COUNT - 1 || ctx.isEmpty()) {
                    builder.append(";");
                    break;
                  } else {
                    builder.append(",");
                  }
                }
                if (builder.length() <= 0) {
                  continue;
                }
                String result = builder.toString();
                ctx.incBytesWrite(result.length());
                writer.write(result);
                writer.newLine();
                writer.flush();
              }
              writer.flush();
              System.out.println("Finished writing file " + ctx.currentWriteFile);
            } catch (Exception e) {
              e.printStackTrace();
            }
          }
        }));
  }

  private BufferedWriter getFileBufferedWriter(Context context)
      throws IOException {
    String nextFile = context.nextFileName();
    context.setCurrentWriteFile(nextFile);
    Path path = Paths.get(OUTPUT_DIR + nextFile);
    return Files.newBufferedWriter(path);
  }
}
