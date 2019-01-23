package dyoon.innocent.temp;

import com.google.common.math.Stats;
import dyoon.innocent.database.ImpalaDatabase;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Dong Young Yoon on 2018-12-10.
 *
 * <p>Contains misc main methods for testing purposes
 */
public class TempMain {

  public static void main(String[] args) {
    test2(args);
  }

  private static void test1(String[] args) {
    String sampleResultTableTemplate =
        "query42_sample_store_sales___st___ss_sold_date_sk___10000___%d";
    String originalResultTable = "query42_orig";

    ImpalaDatabase database =
        new ImpalaDatabase(
            "c220g2-011018.wisc.cloudlab.us:21050",
            "tpcds_500_parquet_result",
            "tpcds_500_parquet_result_innocent",
            "",
            "");

    // get query results with 20 samples for bootstrap
    Map<Integer, List<Double>> results = new HashMap<>();
    Map<Integer, Double> actualResults = new HashMap<>();

    String sql = String.format("SELECT i_category_id, _c3 from %s", originalResultTable);
    try {
      ResultSet rs = database.executeQuery(sql);
      while (rs.next()) {
        int id = rs.getInt(1);
        double value = rs.getDouble(2);

        actualResults.put(id, value);
      }
    } catch (SQLException e) {
      e.printStackTrace();
    }

    for (int i = 1; i <= 100; ++i) {
      String resultTable = String.format(sampleResultTableTemplate, i);
      sql = String.format("SELECT i_category_id, agg%d FROM %s", 0, resultTable);
      try {
        ResultSet rs = database.executeQuery(sql);
        while (rs.next()) {
          int id = rs.getInt(1);
          double value = rs.getDouble(2);

          if (!results.containsKey(id)) {
            results.put(id, new ArrayList<>());
          }
          results.get(id).add(value);
        }
      } catch (SQLException e) {
        e.printStackTrace();
      }
    }

    for (List<Double> list : results.values()) {
      Collections.sort(list);
    }

    double sum = 0;
    int correctCount = 0;
    System.out.println("Bootstrap with 100 samples");
    System.out.println("-------------------------------------");
    for (int i = 1; i <= 10; ++i) {
      double actual = actualResults.get(i);
      List<Double> sampleResults = results.get(i);
      double sampleMean = Stats.meanOf(sampleResults);
      double ub = 2 * sampleMean - (sampleResults.get(1) + sampleResults.get(2)) / 2;
      double lb = 2 * sampleMean - (sampleResults.get(96) + sampleResults.get(97)) / 2;
      sum += (ub - lb);

      if (actual > lb && actual < ub) {
        ++correctCount;
      }
      //      System.out.println(String.format("%d,%.2f,%.2f,%.2f,%.2f", i, actual, lb, sampleMean,
      // ub));
    }
    System.out.println(String.format("average CI width = %.2f", sum / 10));
    System.out.println(String.format("correct count = %d", correctCount));

    double z = 1.96;
    List<Double> widths = new ArrayList<>();
    List<Double> correctness = new ArrayList<>();
    for (int n = 1; n <= 100; n++) {
      String resultTable = String.format(sampleResultTableTemplate, n);
      sql = String.format("SELECT i_category_id, agg%d, agg%d_error FROM %s", 0, 0, resultTable);
      Map<Integer, Double> res = new HashMap<>();
      Map<Integer, Double> err = new HashMap<>();
      try {
        ResultSet rs = database.executeQuery(sql);
        while (rs.next()) {
          int id = rs.getInt(1);
          double value = rs.getDouble(2);
          double error = rs.getDouble(3);
          res.put(id, value);
          err.put(id, error);
        }
      } catch (SQLException e) {
        e.printStackTrace();
      }

      correctCount = 0;
      for (int i = 1; i <= 10; ++i) {
        double actual = actualResults.get(i);
        Double sampleVal = res.get(i);
        Double error = err.get(i);
        double lb = sampleVal - z * error;
        double ub = sampleVal + z * error;
        widths.add(ub - lb);
        if (actual > lb && actual < ub) {
          ++correctCount;
        }
        //        System.out.println(String.format("%d,%.2f,%.2f,%.2f,%.2f", i, actual, lb,
        // sampleVal, ub));
      }
      correctness.add((double) correctCount / 10.0);
    }

    System.out.println("Closed-form using 1 sample");
    System.out.println("-------------------------------------");
    System.out.printf("Average CI width = %.2f\n", Stats.meanOf(widths));
    System.out.printf("Average correctness = %.2f\n", Stats.meanOf(correctness));
    Collections.sort(correctness);
    System.out.printf("Min correctness = %.2f\n", correctness.get(0));
    System.out.printf("Max correctness = %.2f\n", correctness.get(correctness.size() - 1));

    System.exit(0);
  }

  private static void test2(String[] args) {

    double z = 2.645;
    List<Integer> sampleSizeList = Arrays.asList(10000);
    for (int sampleSize : sampleSizeList) {
      String sampleResultTableTemplate =
          "query42_sum_sample_store_sales___st___ss_sold_date_sk___%d___%d";
      String originalResultTable = "query42_sum_orig";

      ImpalaDatabase database =
          new ImpalaDatabase(
              "c220g2-011018.wisc.cloudlab.us:21050",
              "tpcds_500_parquet_result",
              "tpcds_500_parquet_result_innocent",
              "",
              "");

      // get query results with 20 samples for bootstrap
      List<Double> results = new ArrayList<>();
      double actualResult = 0;

      String sql = String.format("SELECT sum_price from %s", originalResultTable);
      try {
        ResultSet rs = database.executeQuery(sql);
        if (rs.next()) {
          actualResult = rs.getDouble(1);
        }
      } catch (SQLException e) {
        e.printStackTrace();
      }

      for (int i = 1; i <= 100; ++i) {
        String resultTable = String.format(sampleResultTableTemplate, sampleSize, i);
        sql = String.format("SELECT sum_price FROM %s", resultTable);
        try {
          ResultSet rs = database.executeQuery(sql);
          while (rs.next()) {
            double value = rs.getDouble(1);

            results.add(value);
          }
        } catch (SQLException e) {
          e.printStackTrace();
        }
      }

      Collections.sort(results);

      int correctCount = 0;
      System.out.println("Bootstrap with 100 samples");
      System.out.println("-------------------------------------");
      double actual = actualResult;
      double sampleMean = Stats.meanOf(results);
      double ub = 2 * sampleMean - results.get(0);
      double lb = 2 * sampleMean - results.get(results.size() - 1);
      if (actual > lb && actual < ub) {
        ++correctCount;
      }
      System.out.println(String.format("CI width = %.2f", ub - lb));
      System.out.println(String.format("correct count = %d", correctCount));

      double value = 0, error = 0;
      List<Double> widths = new ArrayList<>();
      List<Double> correctness = new ArrayList<>();
      for (int n = 1; n <= 100; n++) {
        String resultTable = String.format(sampleResultTableTemplate, sampleSize, n);
        sql = String.format("SELECT sum_price, sum_price_error FROM %s", resultTable);
        try {
          ResultSet rs = database.executeQuery(sql);
          if (rs.next()) {
            value = rs.getDouble(1);
            error = rs.getDouble(2);
          }
        } catch (SQLException e) {
          e.printStackTrace();
        }

        correctCount = 0;
        Double sampleVal = value;
        double lb2 = sampleVal - z * error;
        double ub2 = sampleVal + z * error;
        widths.add(ub2 - lb2);
        if (actualResult > lb2 && actualResult < ub2) {
          ++correctCount;
        }
        correctness.add((double) correctCount / 1.0);
      }

      System.out.println(String.format("Closed-form using 1 sample (sz = %d)", sampleSize));
      System.out.println("-------------------------------------");
      System.out.printf("Average CI width = %.2f\n", Stats.meanOf(widths));
      System.out.printf("Average correctness = %.2f\n", Stats.meanOf(correctness));
    }
    //    Collections.sort(correctness);
    //    System.out.printf("Min correctness = %.2f\n", correctness.get(0));
    //    System.out.printf("Max correctness = %.2f\n", correctness.get(correctness.size() - 1));

    System.exit(0);
  }
}
