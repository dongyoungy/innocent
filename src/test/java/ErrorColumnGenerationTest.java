import dyoon.innocent.ErrorColumnGenerator;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/** Created by Dong Young Yoon on 11/20/18. */
public class ErrorColumnGenerationTest {

  private static Map<SqlIdentifier, SqlIdentifier> sourceToErrorMap = new HashMap<>();

  private static ErrorColumnGenerator gen;

  @BeforeClass
  public static void setup() {
    sourceToErrorMap.put(
        new SqlIdentifier("C1", SqlParserPos.ZERO), new SqlIdentifier("C1_err", SqlParserPos.ZERO));
    sourceToErrorMap.put(
        new SqlIdentifier("C2", SqlParserPos.ZERO), new SqlIdentifier("C2_err", SqlParserPos.ZERO));
    sourceToErrorMap.put(
        new SqlIdentifier("C3", SqlParserPos.ZERO), new SqlIdentifier("C3_err", SqlParserPos.ZERO));
    sourceToErrorMap.put(
        new SqlIdentifier("C4", SqlParserPos.ZERO), new SqlIdentifier("C4_err", SqlParserPos.ZERO));
    gen = new ErrorColumnGenerator(sourceToErrorMap);
  }

  @Test
  public void plusTest() throws SqlParseException {
    String sql = "SELECT c1 + 1";
    SqlParser sqlParser = SqlParser.create(sql);
    SqlNode node = sqlParser.parseQuery();

    SqlNode error = gen.generateErrorColumn(node);
    assertEquals("SELECT `C1_err`", error.toString());
  }

  @Test
  public void plus2Test() throws SqlParseException {
    String sql = "SELECT c1 + c2";
    SqlParser sqlParser = SqlParser.create(sql);
    SqlNode node = sqlParser.parseQuery();

    SqlNode error = gen.generateErrorColumn(node);
    assertEquals("SELECT SQRT(POWER(`C1_err`, 2) + POWER(`C2_err`, 2))", error.toString());
  }

  @Test
  public void plus3Test() throws SqlParseException {
    String sql = "SELECT c1 + c2 + c3";
    SqlParser sqlParser = SqlParser.create(sql);
    SqlNode node = sqlParser.parseQuery();

    SqlNode error = gen.generateErrorColumn(node);
    assertEquals(
        "SELECT SQRT(POWER(SQRT(POWER(`C1_err`, 2) + POWER(`C2_err`, 2)), 2) + "
            + "POWER(`C3_err`, 2))",
        error.toString());
  }

  @Test
  public void plus4Test() throws SqlParseException {
    String sql = "SELECT c1 + c2 + c3 + c4";
    SqlParser sqlParser = SqlParser.create(sql);
    SqlNode node = sqlParser.parseQuery();

    SqlNode error = gen.generateErrorColumn(node);
    assertEquals(
        "SELECT SQRT(POWER(SQRT(POWER(SQRT(POWER(`C1_err`, 2) + POWER(`C2_err`, 2)), 2) + "
            + "POWER(`C3_err`, 2)), 2) + POWER(`C4_err`, 2))",
        error.toString());
  }

  @Test
  public void minusTest() throws SqlParseException {
    String sql = "SELECT c1 - 1";
    SqlParser sqlParser = SqlParser.create(sql);
    SqlNode node = sqlParser.parseQuery();

    SqlNode error = gen.generateErrorColumn(node);
    assertEquals("SELECT `C1_err`", error.toString());
  }

  @Test
  public void minus2Test() throws SqlParseException {
    String sql = "SELECT c1 - c2";
    SqlParser sqlParser = SqlParser.create(sql);
    SqlNode node = sqlParser.parseQuery();

    SqlNode error = gen.generateErrorColumn(node);
    assertEquals("SELECT SQRT(POWER(`C1_err`, 2) + POWER(`C2_err`, 2))", error.toString());
  }

  @Test
  public void multiplyTest() throws SqlParseException {
    String sql = "SELECT c1 * 2";
    SqlParser sqlParser = SqlParser.create(sql);
    SqlNode node = sqlParser.parseQuery();

    SqlNode error = gen.generateErrorColumn(node);
    assertEquals("SELECT `C1_err` * 2", error.toString());
  }

  @Test
  public void multiply2Test() throws SqlParseException {
    String sql = "SELECT c1 * c2";
    SqlParser sqlParser = SqlParser.create(sql);
    SqlNode node = sqlParser.parseQuery();

    SqlNode error = gen.generateErrorColumn(node);
    assertEquals(
        "SELECT `C1` * (`C2` * SQRT(POWER(`C1_err` / `C1`, 2) + POWER(`C2_err` / `C2`, 2)))",
        error.toString());
  }

  @Test
  public void multiply3Test() throws SqlParseException {
    String sql = "SELECT c1 * c2 * c3";
    SqlParser sqlParser = SqlParser.create(sql);
    SqlNode node = sqlParser.parseQuery();

    SqlNode error = gen.generateErrorColumn(node);
    assertEquals(
        "SELECT `C1` * `C2` * (`C3` * SQRT(POWER(`C1` * (`C2` * SQRT(POWER(`C1_err` / `C1`, 2) + "
            + "POWER(`C2_err` / `C2`, 2))) / (`C1` * `C2`), 2) + POWER(`C3_err` / `C3`, 2)))",
        error.toString());
  }

  @Test
  public void multiply4Test() throws SqlParseException {
    String sql = "SELECT c1 * c2 * c3 * c4";
    SqlParser sqlParser = SqlParser.create(sql);
    SqlNode node = sqlParser.parseQuery();

    SqlNode error = gen.generateErrorColumn(node);
    assertEquals(
        "SELECT `C1` * `C2` * `C3` * (`C4` * SQRT(POWER(`C1` * `C2` * (`C3` * "
            + "SQRT(POWER(`C1` * (`C2` * SQRT(POWER(`C1_err` / `C1`, 2) + POWER(`C2_err` / `C2`, 2))) "
            + "/ (`C1` * `C2`), 2) + POWER(`C3_err` / `C3`, 2))) / (`C1` * `C2` * `C3`), 2) + "
            + "POWER(`C4_err` / `C4`, 2)))",
        error.toString());
  }

  @Test
  public void sumTest() throws SqlParseException {
    String sql = "SELECT sum(c1)";
    SqlParser sqlParser = SqlParser.create(sql);
    SqlNode node = sqlParser.parseQuery();

    SqlNode error = gen.generateErrorColumn(node);
    assertEquals("SELECT SQRT(SUM(POWER(`C1_err`, 2)))", error.toString());
  }

  @Test
  public void sum2Test() throws SqlParseException {
    String sql = "SELECT sum(c1 + c2)";
    SqlParser sqlParser = SqlParser.create(sql);
    SqlNode node = sqlParser.parseQuery();

    SqlNode error = gen.generateErrorColumn(node);
    assertEquals(
        "SELECT SQRT(SUM(POWER(SQRT(POWER(`C1_err`, 2) + POWER(`C2_err`, 2)), 2)))",
        error.toString());
  }

  @Test
  public void sum3Test() throws SqlParseException {
    String sql = "SELECT sum(c1 + c2 + c3)";
    SqlParser sqlParser = SqlParser.create(sql);
    SqlNode node = sqlParser.parseQuery();

    SqlNode error = gen.generateErrorColumn(node);
    assertEquals(
        "SELECT SQRT(SUM(POWER(SQRT(POWER(SQRT(POWER(`C1_err`, 2) + POWER(`C2_err`, 2)), 2) + "
            + "POWER(`C3_err`, 2)), 2)))",
        error.toString());
  }

  @Test
  public void averageTest() throws SqlParseException {
    String sql = "SELECT avg(c1)";
    SqlParser sqlParser = SqlParser.create(sql);
    SqlNode node = sqlParser.parseQuery();

    SqlNode error = gen.generateErrorColumn(node);
    assertEquals("SELECT SQRT(SUM(POWER(`C1_err`, 2))) / COUNT(*)", error.toString());
  }

  @Test
  public void sumOverTest() throws SqlParseException {
    String sql = "SELECT sum(c1) over (partition by c2)";
    SqlParser sqlParser = SqlParser.create(sql);
    SqlNode node = sqlParser.parseQuery();

    SqlNode error = gen.generateErrorColumn(node);
    assertEquals("", error.toString());
  }

  @Test
  public void averageOverTest() throws SqlParseException {
    String sql = "SELECT avg(c1) over (partition by c2)";
    SqlParser sqlParser = SqlParser.create(sql);
    SqlNode node = sqlParser.parseQuery();

    SqlNode error = gen.generateErrorColumn(node);
    assertEquals(
        "SELECT SQRT(SUM(POWER(`C1`, 2)) OVER (PARTITION BY `C2`)) / "
            + "(COUNT(*) OVER (PARTITION BY `C2`))",
        error.toString());
  }

  @Test
  public void complex1Test() throws SqlParseException {

    String sql = "SELECT sum(c1) + sum(c2) + sum(c3)";
    SqlParser sqlParser = SqlParser.create(sql);
    SqlNode node = sqlParser.parseQuery();

    SqlNode error = gen.generateErrorColumn(node);
    assertEquals(
        "SELECT SQRT(POWER(SQRT(POWER(SQRT(SUM(POWER(`C1_err`, 2))), 2) + "
            + "POWER(SQRT(SUM(POWER(`C2_err`, 2))), 2)), 2) + "
            + "POWER(SQRT(SUM(POWER(`C3_err`, 2))), 2))",
        error.toString());
  }
}
