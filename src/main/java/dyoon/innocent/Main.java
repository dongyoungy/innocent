package dyoon.innocent;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import com.google.common.io.Files;
import dyoon.innocent.database.DatabaseImpl;
import dyoon.innocent.database.ImpalaDatabase;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.dialect.HiveSqlDialect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.paukov.combinatorics3.Generator;
import org.pmw.tinylog.Logger;

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

    DatabaseImpl database = new ImpalaDatabase(host, db, user, password);
    Data data = new Data();
    Parser p = new Parser();
    File current;
    try {

      // temporary section for testing query modification
      if (args.isTest()) {

        int sampleCount = 0;
        SqlDialect dialect = new HiveSqlDialect(SqlDialect.EMPTY_CONTEXT);

        //        File tpcdsQueryDir = new File(args.getQueryDir());
        //        File[] queryFiles = tpcdsQueryDir.listFiles();
        //        Arrays.sort(queryFiles);
        //        for (File file : queryFiles) {
        //          if (file.isFile()) {
        //            if (file.getName().endsWith("sql")) {
        //              Logger.info("Parsing: {}", file.getName());
        //              String sql = Files.asCharSource(file, Charset.forName("UTF-8")).read();
        //              sql = sql.replaceAll(";", "");
        //              Logger.info(sql);
        //              Parser parser = new Parser();
        //              QueryVisitor v = new QueryVisitor();
        //              Sample s =
        //                  new Sample(
        //                      Sample.Type.STRATIFIED,
        //                      "store_sales",
        //                      Arrays.asList("ss_sold_date_sk"),
        //                      100000);
        //              SqlNode node = parser.parse(sql, v, s);
        //              if (parser.isSampleUsed()) {
        //                Logger.info("Sample used for {}", file.getName());
        //                Logger.info("Rewritten query is {}", node.toSqlString(dialect));
        //                ++sampleCount;
        //              }
        //            }
        //          }
        //        }

        String sql =
            "with ss as\n"
                + " (select ca_county,d_qoy, d_year,sum(ss_ext_sales_price) as store_sales\n"
                + " from store_sales,date_dim,customer_address\n"
                + " where ss_sold_date_sk = d_date_sk\n"
                + "  and ss_addr_sk=ca_address_sk\n"
                + " group by ca_county,d_qoy, d_year),\n"
                + " ws as\n"
                + " (select ca_county,d_qoy, d_year,sum(ws_ext_sales_price) as web_sales\n"
                + " from web_sales,date_dim,customer_address\n"
                + " where ws_sold_date_sk = d_date_sk\n"
                + "  and ws_bill_addr_sk=ca_address_sk\n"
                + " group by ca_county,d_qoy, d_year)\n"
                + " select \n"
                + "        ss1.ca_county\n"
                + "       ,ss1.d_year\n"
                + "       ,ws2.web_sales/ws1.web_sales web_q1_q2_increase\n"
                + "       ,ss2.store_sales/ss1.store_sales store_q1_q2_increase\n"
                + "       ,ws3.web_sales/ws2.web_sales web_q2_q3_increase\n"
                + "       ,ss3.store_sales/ss2.store_sales store_q2_q3_increase\n"
                + " from\n"
                + "        ss ss1\n"
                + "       ,ss ss2\n"
                + "       ,ss ss3\n"
                + "       ,ws ws1\n"
                + "       ,ws ws2\n"
                + "       ,ws ws3\n"
                + " where\n"
                + "    ss1.d_qoy = 1\n"
                + "    and ss1.d_year = 2000\n"
                + "    and ss1.ca_county = ss2.ca_county\n"
                + "    and ss2.d_qoy = 2\n"
                + "    and ss2.d_year = 2000\n"
                + " and ss2.ca_county = ss3.ca_county\n"
                + "    and ss3.d_qoy = 3\n"
                + "    and ss3.d_year = 2000\n"
                + "    and ss1.ca_county = ws1.ca_county\n"
                + "    and ws1.d_qoy = 1\n"
                + "    and ws1.d_year = 2000\n"
                + "    and ws1.ca_county = ws2.ca_county\n"
                + "    and ws2.d_qoy = 2\n"
                + "    and ws2.d_year = 2000\n"
                + "    and ws1.ca_county = ws3.ca_county\n"
                + "    and ws3.d_qoy = 3\n"
                + "    and ws3.d_year =2000\n"
                + "    and case when ws1.web_sales > 0 then ws2.web_sales/ws1.web_sales else null end \n"
                + "       > case when ss1.store_sales > 0 then ss2.store_sales/ss1.store_sales else null end\n"
                + "    and case when ws2.web_sales > 0 then ws3.web_sales/ws2.web_sales else null end\n"
                + "       > case when ss2.store_sales > 0 then ss3.store_sales/ss2.store_sales else null end\n"
                + " order by ss1.d_year";
        Parser parser = new Parser();
        QueryVisitor v = new QueryVisitor();
        Sample s =
            new Sample(
                Sample.Type.STRATIFIED, "store_sales", Arrays.asList("ss_sold_date_sk"), 100000);
        SqlNode node = parser.parse(sql, v, s);
        if (parser.isSampleUsed()) {
          Logger.info("Rewritten query is {}", node.toSqlString(dialect));
          ++sampleCount;
        }

        Logger.info("Sample used = {}", sampleCount);

        System.exit(0);
      }

      for (String table : database.getTables()) {
        data.addTable(table);
        List<String> columns = database.getColumns(table);
        for (String column : columns) {
          data.addColumnToTable(table, column);
        }
      }

      File tpcdsQueryDir = new File(args.getQueryDir());
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

            QueryVisitor visitor = new QueryVisitor();
            p.parse(sql, visitor);
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
            Logger.info("Will create stratified samples with min rows = {}", val);
            minRows.add(val);
          }

          for (SortedSet<String> sampleColumns : set) {
            for (Integer minRow : minRows) {
              Sample s = new Sample(Sample.Type.STRATIFIED, "store_sales", sampleColumns, minRow);
              Logger.info("Creating sample: {}", s.getSampleTableName());
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
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    }

    System.out.println("Done.");
  }
}
