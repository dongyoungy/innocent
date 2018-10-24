package dyoon.innocent;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import com.google.common.io.Files;
import dyoon.innocent.database.DatabaseImpl;
import dyoon.innocent.database.ImpalaDatabase;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.parser.SqlParseException;
import org.paukov.combinatorics3.Generator;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
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
    //    String sql =
    //        "WITH ss as (SELECT * from (SELECT * from t1 as ttt, t2, t3 where ttt.id = t2.id and
    // t3.id = t2.id) tmp), st as (select * from t3 join t4 on t3.c3 = t4.c4) SELECT count(*) as cnt
    // from customer where c_key = 1 and c_id = 2 group by c1, c2 order by c3 desc";

    String host = "c220g5-110932.wisc.cloudlab.us:21050";
    String db = "tpcds_500_parquet";
    String user = "";
    String password = "";

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

    DatabaseImpl database = new ImpalaDatabase(host, db, user, password);
    Data data = new Data();
    Parser p = new Parser();
    File current;
    try {
      for (String table : database.getTables()) {
        data.addTable(table);
        List<String> columns = database.getColumns(table);
        for (String column : columns) {
          data.addColumnToTable(table, column);
        }
      }

      File tpcdsQueryDir = new File("/Users/dyoon/work/impala-tpcds-kit/queries");
      File[] queryFiles = tpcdsQueryDir.listFiles();
      Arrays.sort(queryFiles);
      for (File file : queryFiles) {
        if (file.isFile()) {
          if (file.getName().endsWith("sql")) {
            System.out.println("Parsing: " + file.getName());
            String sql = Files.asCharSource(file, Charset.forName("UTF-8")).read();
            sql = sql.replaceAll(";", "");
            //            sql = "WITH ss as (SELECT count(*) from (SELECT * from t1 as ttt, t2, t3
            // where ttt.id = t2.id and "
            //                    + "t3.id = t2.id) tmp), st as (select * from t3 join t4 on t3.c3 =
            // t4.c4) SELECT count(*) as cnt "
            //                    + "from customer where c_key = 1 and c_id = 2 group by c1, c2
            // order by c3 desc";

            QueryVisitor visitor = p.parse(sql);
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

      List<String> storeSalesCols = new ArrayList<>();
      for (Map.Entry<String, Integer> colFreq : columnFreqList) {
        if (colFreq.getKey().toLowerCase().startsWith("ss_")) {
          storeSalesCols.add(colFreq.getKey().toLowerCase());
        }
      }

      // Let's only build samples for top-N columns
      int count = 0;
      Set<String> columnsForStratified = new HashSet<>();
      for (String col : storeSalesCols) {
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
            System.out.println("Will create stratified samples with min rows = " + val);
            minRows.add(val);
          }

          for (SortedSet<String> sampleColumns : set) {
            for (Integer minRow : minRows) {
              Sample s = new Sample(Sample.Type.STRATIFIED, "store_sales", sampleColumns, minRow);
              System.out.println("Creating sample: " + s.getSampleTableName());
              database.createStratifiedSample(db, s);
            }
          }
        }
      }

      System.out.println("Done.");
    } catch (SqlParseException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    } catch (SQLException e) {
      e.printStackTrace();
    }

    System.out.println("Done.");
  }
}
