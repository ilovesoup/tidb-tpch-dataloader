package com.pingcap.tidb;

import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.io.FileUtils;

public class Transformer {
  private static final Object CTX_LOCK = new Object();
  private static Map<String, int[]> literalNullMap = new HashMap<>();
  private static Map<String, int[]> timestampMap = new HashMap<>();
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
    private String dbName;
    private String format;
    private List<Integer> literalNullColIdx;
    private List<Integer> timestampColIdx;

    Context(String inputFileName, long max_bytes_per_file, BlockingQueue<String[]> queue, Status status, String dbName, String format) {
      this.inputFileName = inputFileName;
      MAX_BYTES_PER_FILE = max_bytes_per_file;
      this.dataQueue = queue;
      this.status = status;
      writeFileIdx = new AtomicInteger(1);
      bytesWrite = new AtomicLong(0);
      this.dbName = dbName;
      this.format = format;

      int[] tmp = literalNullMap.get(inputFileName);
      if (tmp != null) {
        literalNullColIdx = new ArrayList<>();
        for (int i : tmp) {
          literalNullColIdx.add(i);
        }
        Collections.sort(literalNullColIdx);
      }

      tmp = timestampMap.get(inputFileName);
      if (tmp != null) {
        timestampColIdx = new ArrayList<>();
        for (int i : tmp) {
          timestampColIdx.add(i);
        }
        Collections.sort(timestampColIdx);
      }
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
      return dbName;
    }

    public void setDbName(String dbName) {
      this.dbName = dbName;
    }

    String getTableName() {
      return inputFileName.replace("." + format, "");
    }

    String nextFileName() {
      return String.format("%s.%s.%09d.sql",
          getDBName(),
          getTableName(),
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
  private boolean NO_QUOTE = false;
  private int MAX_ROWS_COUNT = 10000;
  private static final long MB = 1024 * 1024;
  private long MAX_BYTES_PER_FILE = 100 * MB; // Default to 100MB
  private int numReaders;
  private int numWriters;
  private static final String[] sepLst = new String[]{
      "\\|",
      ",",
      "\\t"
  };
  private String sep = sepLst[0];
  private String format = "tbl";

  private Collection<File> sources;
  private Map<String, Context> contextMap = new ConcurrentHashMap<>();
  private BlockingQueue<Context> readingCtxQueue = new LinkedBlockingDeque<>();
  private CountDownLatch writerLatch;

  private ExecutorService readers;
  private ExecutorService writers;

  private Options options = new Options();
  private Date startTime;
  private String dbName = "tpch";

  public static void main(String[] args) throws ParseException {
    Transformer transformer = new Transformer();
    CommandLineParser parser = new DefaultParser();
    CommandLine cmd = parser.parse(transformer.getOptions(), args);
    if (cmd.hasOption("help") || args.length <= 0) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp("java -jar tidb-tpch-dataloader-1.0-SNAPSHOT-jar-with-dependencies.jar [option]<arg>\nTPCH data transformer - CSV format to mydumper SQL files.\n", transformer.getOptions());
      System.exit(0);
    }
    if (cmd.hasOption("chunkFileSize")) {
      transformer.MAX_BYTES_PER_FILE = MB * Long.parseLong(cmd.getOptionValue("chunkFileSize"));
    }
    if (cmd.hasOption("noquote")) {
      transformer.NO_QUOTE = true;
    }
    if (cmd.hasOption("outputDir")) {
      String dir = cmd.getOptionValue("outputDir");
      transformer.OUTPUT_DIR = dir.endsWith("/") ? dir : dir + "/";
      System.out.println("Output dir:\t" + transformer.OUTPUT_DIR);
    } else {
      System.out.println("You must specify -outputDir.");
      System.exit(0);
    }
    if (cmd.hasOption("tpchDir")) {
      String dir = cmd.getOptionValue("tpchDir");
      transformer.TPCH_DIR = dir.endsWith("/") ? dir : dir + "/";
      System.out.println("TPCH data dir:\t" + transformer.TPCH_DIR);
    } else {
      System.out.println("You must specify -tpchDir.");
      System.exit(0);
    }
    if (cmd.hasOption("rowCount")) {
      transformer.MAX_ROWS_COUNT = Integer.parseInt(cmd.getOptionValue("rowCount"));
    }
    if (cmd.hasOption("dbName")) {
      transformer.dbName = cmd.getOptionValue("dbName");
    }
    if (cmd.hasOption("format")) {
      transformer.format = cmd.getOptionValue("format");
    }
    if (cmd.hasOption("separator")) {
      int s = Integer.parseInt(cmd.getOptionValue("separator"));
      if (s > sepLst.length - 1) {
        System.out.println("Invalid separator option, please refer to help.");
      } else {
        transformer.sep = sepLst[s];
      }
    }
    int readers = 2;
    int writers = 2;
    if (cmd.hasOption("readers")) {
      readers = Integer.parseInt(cmd.getOptionValue("readers"));
    }
    if (cmd.hasOption("writers")) {
      writers = Integer.parseInt(cmd.getOptionValue("writers"));
    }
    if (cmd.hasOption("literalNullCols")) {
      String str = cmd.getOptionValue("literalNullCols");
      // file1:[1,2,3,4]#file2:[3,4,5]
      literalNullMap = parseColumnList(str);
    }
    if (cmd.hasOption("timestampCols")) {
      String str = cmd.getOptionValue("timestampCols");
      // file1:[1,2,3,4]#file2:[3,4,5]
      timestampMap = parseColumnList(str);
    }

    transformer.run(readers, writers);
  }

  private static Map<String, int[]> parseColumnList(String opt) {
    Map<String, int[]> resultMap = new HashMap<>();
    try {
      String[] files = opt.split("#");
      for (String file : files) {
        String[] parts = file.split(":");
        assert parts.length == 2;
        String fileName = parts[0];
        String specialCols = parts[1];
        String[] cols = specialCols.split(",");
        int[] colList = new int[cols.length];
        for (int i = 0; i < cols.length; i++) {
          colList[i] = Integer.parseInt(cols[i].replace("[", "").replace("]", ""));
        }
        resultMap.put(fileName, colList);
      }
      return resultMap;
    } catch (Exception e) {
      throw new RuntimeException("Invalid literalNullCols option, please refer to help.", e);
    }
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
    options.addOption("format", true, "Your csv format(like 'csv', 'tbl', etc.)");
    options.addOption("noquote", false, "Add no quote around value");
    options.addOption("separator", true, "Defines how csv file is separated: 0 for '|' 1 for ',' 2 for '\t'(tab)");
    options.addOption("tpchDir", true, "Directory where you place your tpch data file in.");
    options.addOption("outputDir", true, "Directory where the transformed sql files will be placed in.");
    options.addOption("rowCount", true, "How many rows per `INSERT INTO` statement.");
    options.addOption("chunkFileSize", true, "Split tables into chunks of this output file size. This value is in MB.");
    options.addOption("readers", true, "Reader thread count.(one thread per file)");
    options.addOption("writers", true, "Writer thread count.(one thread per file)");
    options.addOption("dbName", true, "Database name:tpch/tpch_idx");
    options.addOption("literalNullCols", true, "Columns to insert literal null instead of quoted \"null\"(filename1:[col1, col2, ...]#filename2...) eg. \"lineitem.csv:[1,2]#user.csv:[3,5]\"");
    options.addOption("timestampCols", true, "Columns to insert as timestamp (filename1:[col1, col2, ...]#filename2...) eg. \"lineitem.csv:[1,2]#user.csv:[3,5]\"");
  }

  public Options getOptions() {
    return options;
  }

  private void initReaderWriter(int reader, int writer) {
    readers = Executors.newFixedThreadPool(reader);
    writers = Executors.newFixedThreadPool(writer);
    numReaders = reader;
    numWriters = writer;
  }

  private void prepareDataFiles() {
    System.out.println("Processing " + format + " files...");
    sources = FileUtils.listFiles(new File(TPCH_DIR), new String[]{format}, true);
  }

  private void verifyData() {
    if (sources == null) {
      throw new IllegalStateException("Data has not been loaded properly");
    } else {
      sources.stream().map(File::getName).forEach(System.out::println);
      writerLatch = new CountDownLatch(sources.size());
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
                      Context.Status.INITIAL,
                      dbName,
                      format
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
          !writers.awaitTermination(1, TimeUnit.SECONDS)) {
      }
      writeMetaData();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private void writeMetaData() {
    try (BufferedWriter writer = Files.newBufferedWriter(Paths.get(OUTPUT_DIR + "metadata"))) {
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
        long readCnt = 0;
        Context ctx = contextMap.get(fileName);
        Objects.requireNonNull(ctx);
        try (BufferedReader reader = Files.newBufferedReader(path)) {
          ctx.setStatus(Context.Status.READING);
          readingCtxQueue.add(ctx);

          CSVReader csvReader =
              new CSVReaderBuilder(reader)
                  .withSkipLines(0)
                  .withKeepCarriageReturn(false)
                  .build();

          String[] currentLine;
          long start = System.currentTimeMillis();
          while ((currentLine = csvReader.readNext()) != null) {
            readCnt++;
            for (int i = 0; i < currentLine.length; i++) {
              if (currentLine[i] != null) {
                currentLine[i] = currentLine[i].trim();
              }
            }
            ctx.putData(currentLine);
          }
          System.out.println("Finished reading " + readCnt +
              " lines from " + fileName + " using time(ms):" + (System.currentTimeMillis() - start));
        } catch (Exception e) {
          e.printStackTrace();
          exit("Reading file " + fileName + " failed due to " + e.getMessage(), 2);
        } finally {
          ctx.setStatus(Context.Status.FINISHED);
        }
      });
    });
  }

  private void exit(String msg, int status) {
    System.err.println("Internal error:" + msg + ", exiting.");
    System.exit(status);
  }

  private Context nextCtx2Write() {
    synchronized (CTX_LOCK) {
      if (writerLatch.getCount() > 0) {
        writerLatch.countDown();
        try {
          return readingCtxQueue.take();
        } catch (InterruptedException e) {
          e.printStackTrace();
          exit(e.getMessage(), 1);
        }
      }
      return null;
    }
  }

  private void startWriter() {
    for (int num = 0; num < numWriters; num++) {
      writers.submit(() -> {
        Context ctx;
        while ((ctx = nextCtx2Write()) != null) {
          while (!ctx.isEmpty()) {
            try (BufferedWriter writer = getFileBufferedWriter(ctx)) {
              writer.write("/*!40101 SET NAMES binary*/;\n" +
                  "/*!40014 SET FOREIGN_KEY_CHECKS=0*/;\n" +
                  "/*!40103 SET TIME_ZONE='+00:00' */;\n");

              while (true) {
                if (ctx.isEmpty() || ctx.needProceedNextFile()) {
                  break;
                }
                String result = getWriteContext(ctx, writer);
                if (result == null) continue;

                ctx.incBytesWrite(result.length());
                writer.write(result);
                writer.newLine();
                writer.flush();
              }
              writer.flush();
              System.out.println("Finished writing file " + ctx.currentWriteFile);
            } catch (Exception e) {
              e.printStackTrace();
              exit("Convert " + ctx.currentWriteFile + " failed due to " + e.getMessage(), 3);
            }
          }
          System.out.println("Successfully processed " + ctx.getTableName());
        }
      });
    }
  }

  private String getWriteContext(Context ctx, BufferedWriter writer) throws IOException, InterruptedException {
    StringBuilder builder = new StringBuilder();
    for (int i = 0; i < MAX_ROWS_COUNT; i++) {
      // Check whether data queue is pending to finish.
      while (ctx.getDataQueue().isEmpty()) {
        if (ctx.status == Context.Status.FINISHED) {
          if (builder.length() > 0) {
            String res = builder.toString();
            if (res.endsWith(",")) {
              // Replace the last
              return res.substring(0, res.length() - 1) + ";";
            } else {
              // Should never reach here
              throw new RuntimeException("Constructed insert [" + res + "] stmt is corrupted due to unknown reason");
            }
          } else {
            return null;
          }
        }
      }
      if (i == 0) {
        writer.write("INSERT INTO `" + ctx.getTableName() + "` VALUES\n");
      }
      String quote = NO_QUOTE ? "" : "\"";
      // append insert values
      String[] data = ctx.getDataQueue().take();
      String[] rewriteData = new String[data.length];

      for (int j = 0; j < data.length; j++) {
        if (data[j].equalsIgnoreCase("null")) {
          if (ctx.literalNullColIdx.contains(j)) {
            rewriteData[j] = data[j];
          } else {
            rewriteData[j] = quote + data[j] + quote;
          }
        } else if (ctx.timestampColIdx.contains(j)) {
          rewriteData[j] = quote + convertTimestamp(data[j]) + quote;
        } else {
          rewriteData[j] = quote + data[j] + quote;
        }
      }
      builder
          .append("(")
          .append(String.join(",", rewriteData))
          .append(")");


      if (i == MAX_ROWS_COUNT - 1 || ctx.isEmpty()) {
        builder.append(";");
        break;
      } else {
        builder.append(",");
      }
    }
    if (builder.length() <= 0) {
      return null;
    }
    return builder.toString();
  }

  private static final SimpleDateFormat df = new SimpleDateFormat("M/d/yyyy HH:mm:ss");
  private static final SimpleDateFormat dfOut = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

  private static String convertTimestamp(String input) {
    try {
      int dotPos = input.indexOf(".");
      if (dotPos != -1) {
        input = input.substring(0, dotPos);
      }
      input = input.replace("\"", "");
      input = input.trim();

      Date d = df.parse(input);
      return dfOut.format(d);
    } catch (Exception e) {
      throw new RuntimeException(String.format("wrong date format: [%s]", input), e);
    }
  }

  private BufferedWriter getFileBufferedWriter(Context context)
      throws IOException {
    String nextFile = context.nextFileName();
    context.setCurrentWriteFile(nextFile);
    Path path = Paths.get(OUTPUT_DIR + nextFile);
    return Files.newBufferedWriter(path);
  }
}
