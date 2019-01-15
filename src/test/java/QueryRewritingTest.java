import dyoon.innocent.AQPInfo;
import dyoon.innocent.InnocentEngine;
import dyoon.innocent.Query;
import dyoon.innocent.Sample;
import org.apache.calcite.sql.parser.SqlParseException;
import org.junit.Test;

import java.sql.SQLException;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;

/** Created by Dong Young Yoon on 11/18/18. */
public class QueryRewritingTest {

  private Sample s;

  private static final String TABLE_NAME = "t1";

  private static final String[] SAMPLE_COLUMNS = {"c1", "c2"};

  private static final int MIN_ROWS = 100;

  public QueryRewritingTest() {
    s = new Sample(Sample.Type.STRATIFIED, TABLE_NAME, Arrays.asList(SAMPLE_COLUMNS), MIN_ROWS);
  }

  @Test
  public void simpleSumWithoutErrorTest()
      throws ClassNotFoundException, SQLException, SqlParseException {

    InnocentEngine engine = new InnocentEngine();
    String sql = "SELECT SUM(c3) FROM t1";
    Query q = new Query("q1", sql);
    AQPInfo info = engine.rewriteWithSample(q, s, false);
    String aqpQuery = q.getAqpQuery();
    System.out.println(aqpQuery);
    String expected =
        "SELECT SUM(sum0)\n"
            + "FROM (SELECT SUM(c3 * 100000 / stat.groupsize * stat.actualsize / 100000) sum0, t1.c1, t1.c2\n"
            + "FROM t1___st___c1__c2___100 t1\n"
            + "INNER JOIN t1___st___c1__c2___100___stat stat ON t1.c1 = stat.c1 AND t1.c2 = stat.c2\n"
            + "GROUP BY t1.c1, t1.c2) tmp0";
    assertEquals(expected.toLowerCase(), aqpQuery.toLowerCase());
  }

  @Test
  public void simpleSumWithErrorTest()
      throws ClassNotFoundException, SQLException, SqlParseException {

    InnocentEngine engine = new InnocentEngine();
    String sql = "SELECT SUM(c3) FROM t1";
    Query q = new Query("q1", sql);
    AQPInfo info = engine.rewriteWithSample(q, s, true);
    String aqpQuery = q.getAqpQuery();
    System.out.println(aqpQuery);
    String expected =
        "SELECT SUM(sum0), SQRT(SUM(sum0_var * sum0_count + POWER(SUM0_SAMPLEMEAN - SUM0_GROUPMEAN, 2) "
            + "* (sum0_count + 1)) / (SUM(sum0_count + 1) - 1)) * SQRT(SUM(sum0_count + 1)) sum0_0_error, "
            + "SQRT(SUM(sum0_var * sum0_count + POWER(SUM0_SAMPLEMEAN - SUM0_GROUPMEAN, 2) * (sum0_count + 1)) "
            + "/ (SUM(sum0_count + 1) - 1)) * SQRT(SUM(sum0_count + 1)) / SUM(sum0) sum0_0_rel_error\n"
            + "FROM (SELECT SUM(C3 * 100000 / stat.groupsize * stat.actualsize / 100000) sum0, "
            + "t1.c1, t1.c2, AVG(AVG(C3 * 100000 / stat.groupsize * stat.actualsize / 100000)) OVER () sum0_samplemean, "
            + "AVG(C3 * 100000 / stat.groupsize * stat.actualsize / 100000) sum0_groupmean, "
            + "VAR_SAMP(C3 * 100000 / stat.groupsize * stat.actualsize / 100000) sum0_var, COUNT(*) - 1 sum0_count\n"
            + "FROM t1___st___c1__c2___100 t1\n"
            + "INNER JOIN t1___st___c1__c2___100___stat stat ON t1.c1 = stat.c1 AND t1.c2 = stat.c2\n"
            + "GROUP BY t1.c1, t1.c2) tmp0";
    assertEquals(expected.toLowerCase(), aqpQuery.toLowerCase());
  }

  @Test
  public void simpleAvgWithoutErrorTest()
      throws ClassNotFoundException, SQLException, SqlParseException {

    InnocentEngine engine = new InnocentEngine();
    String sql = "SELECT AVG(c3) FROM t1";
    Query q = new Query("q1", sql);
    AQPInfo info = engine.rewriteWithSample(q, s);
    String aqpQuery = q.getAqpQuery();
    System.out.println(aqpQuery);
    String expected =
        "SELECT AVG(avg0)\n"
            + "FROM (SELECT AVG(C3) avg0, t1.c1, t1.c2\n"
            + "FROM t1___st___c1__c2___100 t1\n"
            + "INNER JOIN t1___st___c1__c2___100___stat stat ON t1.c1 = stat.c1 AND t1.c2 = stat.c2\n"
            + "GROUP BY t1.c1, t1.c2) tmp0";
    assertEquals(expected.toLowerCase(), aqpQuery.toLowerCase());
  }

  @Test
  public void simpleCountWithoutErrorTest()
      throws ClassNotFoundException, SQLException, SqlParseException {

    InnocentEngine engine = new InnocentEngine();
    String sql = "SELECT COUNT(*) FROM t1 WHERE c4 < 100";
    Query q = new Query("q1", sql);
    AQPInfo info = engine.rewriteWithSample(q, s, false);
    String aqpQuery = q.getAqpQuery();
    System.out.println(aqpQuery);
    String expected =
        "SELECT SUM(cnt0)\n"
            + "FROM (SELECT COUNT(*) * 100000 / stat.groupsize * stat.actualsize / 100000 cnt0, t1.c1, t1.c2\n"
            + "FROM t1___st___c1__c2___100 t1\n"
            + "INNER JOIN t1___st___c1__c2___100___stat stat ON t1.c1 = stat.c1 AND t1.c2 = stat.c2\n"
            + "WHERE C4 < 100\n"
            + "GROUP BY t1.c1, t1.c2) tmp0";
    assertEquals(expected.toLowerCase(), aqpQuery.toLowerCase());
  }

  @Test
  public void nestedSumWithoutErrorTest()
      throws ClassNotFoundException, SQLException, SqlParseException {

    InnocentEngine engine = new InnocentEngine();
    String sql = "SELECT SUM(s1) FROM (SELECT SUM(c3) as s1 FROM t1 group by c4)";
    Query q = new Query("q1", sql);
    AQPInfo info = engine.rewriteWithSample(q, s, false);
    String aqpQuery = q.getAqpQuery();
    System.out.println(aqpQuery);
    String expected =
        "SELECT SUM(S1)\n"
            + "FROM (SELECT SUM(S1) S1\n"
            + "FROM (SELECT SUM(C3 * 100000 / stat.groupsize * stat.actualsize / 100000) S1, t1.c1, t1.c2\n"
            + "FROM t1___st___c1__c2___100 t1\n"
            + "INNER JOIN t1___st___c1__c2___100___stat stat ON t1.c1 = stat.c1 AND t1.c2 = stat.c2\n"
            + "GROUP BY C4, t1.c1, t1.c2) tmp0\n"
            + "GROUP BY tmp0.C4)";
    assertEquals(expected.toLowerCase(), aqpQuery.toLowerCase());
  }

  @Test
  public void simpleAvgWithErrorTest()
      throws ClassNotFoundException, SQLException, SqlParseException {

    InnocentEngine engine = new InnocentEngine();
    String sql = "SELECT AVG(c3) FROM t1";
    Query q = new Query("q1", sql);
    AQPInfo info = engine.rewriteWithSample(q, s, true);
    String aqpQuery = q.getAqpQuery();
    System.out.println(aqpQuery);
    String expected =
        "SELECT AVG(avg0) avg0, "
            + "SQRT(SUM(avg0_var * avg0_count + POWER(AVG0_SAMPLEMEAN - AVG0_GROUPMEAN, 2) * "
            + "(avg0_count + 1)) / (SUM(avg0_count + 1) - 1)) / SQRT(SUM(avg0_count + 1)) avg0_error, "
            + "SQRT(SUM(avg0_var * avg0_count + POWER(AVG0_SAMPLEMEAN - AVG0_GROUPMEAN, 2) * "
            + "(avg0_count + 1)) / (SUM(avg0_count + 1) - 1)) / SQRT(SUM(avg0_count + 1)) / "
            + "AVG(avg0) avg0_rel_error\n"
            + "FROM (SELECT AVG(C3) avg0, t1.c1, t1.c2, AVG(AVG(C3)) OVER () avg0_samplemean, "
            + "AVG(C3) avg0_groupmean, VAR_SAMP(C3) avg0_var, COUNT(*) - 1 avg0_count\n"
            + "FROM t1___st___c1__c2___100 t1\n"
            + "INNER JOIN t1___st___c1__c2___100___stat stat ON t1.c1 = stat.c1 AND t1.c2 = stat.c2\n"
            + "GROUP BY t1.c1, t1.c2) tmp0";
    assertEquals(expected.toLowerCase(), aqpQuery.toLowerCase());
  }

  @Test
  public void nestedSumWithErrorTest()
      throws ClassNotFoundException, SQLException, SqlParseException {

    InnocentEngine engine = new InnocentEngine();
    String sql = "SELECT SUM(s1) FROM (SELECT SUM(c3) as s1 FROM t1 group by c4)";
    Query q = new Query("q1", sql);
    AQPInfo info = engine.rewriteWithSample(q, s, true);
    String aqpQuery = q.getAqpQuery();
    System.out.println(aqpQuery);
    String expected =
        "SELECT SUM(S1) agg0\n"
            + "FROM (SELECT SUM(S1) S1, SQRT(SUM(S1_var * S1_count + POWER(S1_SAMPLEMEAN - S1_GROUPMEAN, 2) * (S1_count + 1)) / (SUM(S1_count + 1) - 1)) * SQRT(SUM(S1_count + 1)) S1_error, SQRT(SUM(S1_var * S1_count + POWER(S1_SAMPLEMEAN - S1_GROUPMEAN, 2) * (S1_count + 1)) / (SUM(S1_count + 1) - 1)) * SQRT(SUM(S1_count + 1)) / SUM(S1) S1_rel_error\n"
            + "FROM (SELECT SUM(C3 * 100000 / stat.groupsize * stat.actualsize / 100000) S1, t1.c1, t1.c2, C4, AVG(AVG(C3 * 100000 / stat.groupsize * stat.actualsize / 100000)) OVER (PARTITION BY C4) S1_samplemean, AVG(C3 * 100000 / stat.groupsize * stat.actualsize / 100000) S1_groupmean, VAR_SAMP(C3 * 100000 / stat.groupsize * stat.actualsize / 100000) S1_var, COUNT(*) - 1 S1_count\n"
            + "FROM t1___st___c1__c2___100 t1\n"
            + "INNER JOIN t1___st___c1__c2___100___stat stat ON t1.c1 = stat.c1 AND t1.c2 = stat.c2\n"
            + "GROUP BY C4, t1.c1, t1.c2) tmp0\n"
            + "GROUP BY tmp0.C4)";
    assertEquals(expected.toLowerCase(), aqpQuery.toLowerCase());
  }

  @Test
  public void nestedSumWithErrorPropagationTest()
      throws ClassNotFoundException, SQLException, SqlParseException {

    InnocentEngine engine = new InnocentEngine();
    String sql = "SELECT SUM(s1) FROM (SELECT SUM(c3) as s1 FROM t1 group by c4)";
    Query q = new Query("q1", sql);
    AQPInfo info = engine.rewriteWithSample(q, s, true, true);
    String aqpQuery = q.getAqpQuery();
    System.out.println(aqpQuery);
    String expected =
        "SELECT SUM(S1) agg0, SQRT(SUM(POWER(S1_error, 2))) agg0_error\n"
            + "FROM (SELECT SUM(S1) S1, SQRT(SUM(S1_var * S1_count + POWER(S1_SAMPLEMEAN - S1_GROUPMEAN, 2) * (S1_count + 1)) / (SUM(S1_count + 1) - 1)) * SQRT(SUM(S1_count + 1)) S1_error, SQRT(SUM(S1_var * S1_count + POWER(S1_SAMPLEMEAN - S1_GROUPMEAN, 2) * (S1_count + 1)) / (SUM(S1_count + 1) - 1)) * SQRT(SUM(S1_count + 1)) / SUM(S1) S1_rel_error\n"
            + "FROM (SELECT SUM(C3 * 100000 / stat.groupsize * stat.actualsize / 100000) S1, t1.c1, t1.c2, C4, AVG(AVG(C3 * 100000 / stat.groupsize * stat.actualsize / 100000)) OVER (PARTITION BY C4) S1_samplemean, AVG(C3 * 100000 / stat.groupsize * stat.actualsize / 100000) S1_groupmean, VAR_SAMP(C3 * 100000 / stat.groupsize * stat.actualsize / 100000) S1_var, COUNT(*) - 1 S1_count\n"
            + "FROM t1___st___c1__c2___100 t1\n"
            + "INNER JOIN t1___st___c1__c2___100___stat stat ON t1.c1 = stat.c1 AND t1.c2 = stat.c2\n"
            + "GROUP BY C4, t1.c1, t1.c2) tmp0\n"
            + "GROUP BY tmp0.C4)";
    assertEquals(expected.toLowerCase(), aqpQuery.toLowerCase());
  }
}
