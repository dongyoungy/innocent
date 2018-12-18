package dyoon.innocent;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import com.google.common.io.Files;
import dyoon.innocent.database.DatabaseImpl;
import dyoon.innocent.database.ImpalaDatabase;
import dyoon.innocent.query.AggNonAggDetector;
import dyoon.innocent.query.QueryVisitor;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.parser.SqlParseException;
import org.paukov.combinatorics3.Generator;
import org.pmw.tinylog.Configurator;
import org.pmw.tinylog.Level;
import org.pmw.tinylog.Logger;
import org.pmw.tinylog.writers.ConsoleWriter;
import org.pmw.tinylog.writers.FileWriter;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** Created by Dong Young Yoon on 10/19/18. */
public class Main {
  public static void main(String[] argv) {

    Args args = new Args();
    JCommander jc = JCommander.newBuilder().addObject(args).build();

    try {
      jc.parse(argv);
    } catch (ParameterException e) {
      e.getJCommander().usage();
      return;
    }

    if (args.isHelp()) {
      jc.usage();
      return;
    }

    String host = args.getHost();
    String db = args.getDatabase();
    String user = "";
    String password = "";
    String timestamp = new SimpleDateFormat("yyyyMMdd_HHmmss").format(new Date());

    // set log level
    Configurator.currentConfig().level(Level.DEBUG).activate();

    DatabaseImpl database = new ImpalaDatabase(host, db, user, password);
    Data data = new Data();
    InnocentEngine engine = new InnocentEngine(database, args, timestamp);
    File current;

    File logDir = new File(String.format("./log/%s/", timestamp));
    logDir.mkdirs();
    String defalutLogFile = String.format("./log/%s/all.log", timestamp);

    try {

      File tpcdsQueryDir = new File(args.getQueryDir());
      File[] queryFiles = tpcdsQueryDir.listFiles();
      Arrays.sort(queryFiles);
      // temporary section for testing query modification
      if (args.isTestOrigQueries()) {

        File resFile = new File(String.format("./log/%s/orig_result", timestamp));
        PrintWriter pw = new PrintWriter(new FileOutputStream(resFile, true));

        for (File file : queryFiles) {
          if (file.isFile()) {
            String queryFilename = file.getName();
            if (queryFilename.endsWith("sql")) {
              String id = Files.getNameWithoutExtension(queryFilename);
              String sql = Files.asCharSource(file, Charset.defaultCharset()).read();
              sql = sql.replaceAll(";", "");
              Logger.info("Parsing: {}", queryFilename);

              String logFile = String.format("./log/%s/%s.log", timestamp, id);

              Configurator.currentConfig()
                  .writer(new ConsoleWriter(), Level.DEBUG)
                  .addWriter(new FileWriter(logFile), Level.DEBUG)
                  .addWriter(new FileWriter(defalutLogFile), Level.DEBUG)
                  .activate();

              Query q = new Query(id, sql);
              try {
                double time = database.runQueryAndSaveResult(q, args);
                pw.println(String.format("%s,%.4f", q.getId(), time / 1000));
                pw.flush();
              } catch (Exception e) {
                e.printStackTrace();
                System.err.println(String.format("%s failed to run.", q.getId()));
              }
            }
          }
        }
      } else if (args.isCreateDuplicateSamples()) {
        String table = args.getSampleTable();
        String colStr = args.getSampleColumns();
        String[] columns = colStr.split(",");
        long minRow = args.getSampleRows();
        String type = args.getSampleType(); // ignored for now
        long numSample = args.getNumSampleToCreate();

        for (int i = 1; i <= numSample; ++i) {
          Sample s = new Sample(Sample.Type.STRATIFIED, table, Arrays.asList(columns), minRow, i);
          database.createStratifiedSample(args.getDatabaseForInnocent(), args.getDatabase(), s);
        }

      } else if (args.isTest()) {

        List<Sample> samples = new ArrayList<>();
        List<String> tables = database.getTables();
        String[] factTables = args.getFactTables().split(",");

        List<String> factTableList = Arrays.asList(factTables);
        List<Integer> minRows = new ArrayList<>();
        if (!args.getMinRows().isEmpty()) {
          String[] minRowStrings = args.getMinRows().split(",");
          for (String minRow : minRowStrings) {
            int val = Integer.parseInt(minRow.trim());
            minRows.add(val);
          }
        }

        //        for (String table : tables) {
        //          String[] tokens = table.split("___");
        //          if (tokens.length == 4) {
        //            String sampleTable = tokens[0];
        //            if (!factTableList.isEmpty() && !factTableList.contains(sampleTable)) {
        //              continue;
        //            }
        //            String[] columns = tokens[2].split("__");
        //            int minRow = Integer.parseInt(tokens[3]);
        //            if (!minRows.isEmpty() && !minRows.contains(minRow)) {
        //              continue;
        //            }
        //            Sample s =
        //                new Sample(Sample.Type.STRATIFIED, sampleTable, Arrays.asList(columns),
        // minRow);
        //            samples.add(s);
        //          }
        //        }

        // temp
        for (int i = 1; i <= 100; ++i) {
          Sample s1 =
              new Sample(
                  Sample.Type.STRATIFIED,
                  "store_sales",
                  Arrays.asList("ss_sold_date_sk"),
                  10000,
                  i);
          samples.add(s1);
        }

        for (Sample s : samples) {
          int sampleCount = 0;

          for (File file : queryFiles) {
            if (file.isFile()) {
              String queryFilename = file.getName();
              if (queryFilename.endsWith("sql")) {
                String id = Files.getNameWithoutExtension(queryFilename);
                String sql = Files.asCharSource(file, Charset.defaultCharset()).read();
                sql = sql.replaceAll(";", "");

                if (!id.equalsIgnoreCase("query42_sum")) {
                  continue;
                }

                Logger.info("Parsing: {}", queryFilename);

                String logFile = String.format("./log/%s/%s.log", timestamp, id);

                Configurator.currentConfig()
                    .writer(new ConsoleWriter(), Level.DEBUG)
                    .addWriter(new FileWriter(logFile), Level.DEBUG)
                    .addWriter(new FileWriter(defalutLogFile), Level.DEBUG)
                    .activate();

                Query q = new Query(id, sql);
                AQPInfo aqpInfo = engine.rewriteWithSample(q, s, true, true);
                if (aqpInfo != null) {
                  Logger.info("Rewritten query is {}", aqpInfo.getQuery().getAqpQuery());
                  ++sampleCount;

                  //                  ErrorColumnPropagator propagator = new
                  // ErrorColumnPropagator();
                  //                  SqlNode node = aqpInfo.getAqpNode().accept(propagator);
                  //                  aqpInfo.setAqpNode(node);

                  AggNonAggDetector detector = new AggNonAggDetector(aqpInfo);
                  aqpInfo.getAqpNode().accept(detector);
                  List<ColumnType> columnTypeList = detector.getColumnTypeList();
                  aqpInfo.setColumnTypeList(columnTypeList);

                  engine.runAQPQueryAndCompare(q, aqpInfo, args);
                }
              }
            }
          }

          Logger.info("Sample used = {}", sampleCount);
        }
      } else if (args.isDoPartition()) {
        for (File file : queryFiles) {
          if (file.isFile()) {
            String queryFilename = file.getName();
            if (queryFilename.endsWith("sql")) {
              String id = Files.getNameWithoutExtension(queryFilename);
              String sql = Files.asCharSource(file, Charset.defaultCharset()).read();
              sql = sql.replaceAll(";", "");

              //              if (!id.equalsIgnoreCase("query42_partition_test")) {
              //                continue;
              //              }

              String logFile = String.format("./log/%s/%s.log", timestamp, id);

              Configurator.currentConfig()
                  .writer(new ConsoleWriter(), Level.DEBUG)
                  .addWriter(new FileWriter(logFile), Level.DEBUG)
                  .addWriter(new FileWriter(defalutLogFile), Level.DEBUG)
                  .activate();

              Query q = new Query(id, sql);

              engine.runPartitionAnalysis(q);
            }
          }
        }
        engine.findBestColumnsForPartition();
        System.out.println();
      } else {

        if (args.getFactTables().isEmpty()) {
          Logger.warn("Fact tables must be given for analysis.");
          return;
        }

        String[] factTables = args.getFactTables().split(",");

        for (String table : database.getTables()) {
          data.addTable(table);
          List<String> columns = database.getColumns(table);
          for (String column : columns) {
            data.addColumnToTable(table, column);
          }
        }

        for (File file : queryFiles) {
          if (file.isFile()) {
            if (file.getName().endsWith("sql")) {
              System.out.println("Parsing: " + file.getName());
              String sql = Files.asCharSource(file, Charset.forName("UTF-8")).read();
              sql = sql.replaceAll(";", "");

              QueryVisitor visitor = new QueryVisitor();
              engine.parse(sql, visitor);
              for (SqlIdentifier id : visitor.getQueryColumnSet()) {
                data.incrementFreq(id.names.get(id.names.size() - 1));
              }
            }
          }
        }
        Stream<Map.Entry<String, Integer>> sorted =
            data.getColumnFrequency()
                .entrySet()
                .stream()
                .sorted(Collections.reverseOrder(Map.Entry.comparingByValue()));
        List<Map.Entry<String, Integer>> columnFreqList = sorted.collect(Collectors.toList());

        for (String table : factTables) {
          List<String> eligibleColumns = new ArrayList<>();
          List<String> columnsInTable = data.getColumns(table);
          for (Map.Entry<String, Integer> colFreq : columnFreqList) {
            if (Utils.containsIgnoreCase(colFreq.getKey(), columnsInTable)) {
              eligibleColumns.add(colFreq.getKey().toLowerCase());
            }
          }

          // Let's only build samples for top-N columns
          int count = 0;
          Set<String> columnsForStratified = new HashSet<>();
          for (String col : eligibleColumns) {
            if (count < args.getTopNColumns()) columnsForStratified.add(col);
            ++count;
          }

          Set<SortedSet<String>> set = new HashSet<>();
          for (int i = 1; i <= args.getMaxColPerSample(); ++i) {
            Set<List<String>> collect =
                Generator.combination(columnsForStratified)
                    .simple(i)
                    .stream()
                    .collect(Collectors.toSet());
            for (List<String> columns : collect) {
              SortedSet<String> colSet = new TreeSet<>(columns);
              set.add(colSet);
            }
          }

          List<Integer> minRows = new ArrayList<>();
          if (args.isCreate()) {
            if (!args.getMinRows().isEmpty()) {
              String[] minRowStrings = args.getMinRows().split(",");
              for (String minRow : minRowStrings) {
                int val = Integer.parseInt(minRow.trim());
                Logger.info("Will create stratified samples with min rows = {}", val);
                minRows.add(val);
              }

              for (SortedSet<String> sampleColumns : set) {
                for (Integer minRow : minRows) {
                  Sample s = new Sample(Sample.Type.STRATIFIED, table, sampleColumns, minRow);
                  Logger.info("Creating sample: {}", s.getSampleTableName());
                  database.createStratifiedSample(db, s);
                }
              }
            }
          }
        }
      }
    } catch (SqlParseException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    } catch (SQLException e) {
      e.printStackTrace();
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    }

    System.out.println("Done.");
    System.exit(0);
  }
}
