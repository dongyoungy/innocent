import dyoon.innocent.Query;
import dyoon.innocent.Utils;
import dyoon.innocent.data.Column;
import dyoon.innocent.data.EqualPredicate;
import dyoon.innocent.data.FactDimensionJoin;
import dyoon.innocent.data.Predicate;
import dyoon.innocent.data.Table;
import dyoon.innocent.query.AliasExtractor;
import dyoon.innocent.query.JoinAndPredicateFinder;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** Created by Dong Young Yoon on 11/20/18. */
public class QueryParsingTest {

  private static Set<Table> allTables = new HashSet<>();
  private static List<Column> allColumns = new ArrayList<>();

  private static Set<Table> factTables = new HashSet<>();
  private static Set<Table> ignoreFactTables = new HashSet<>();

  @BeforeClass
  public static void setup() {
    Table f1 = new Table("f1");
    Column c1 = new Column(f1, "c1", "int");
    Column c2 = new Column(f1, "c2", "int");
    f1.addColumn(c1);
    f1.addColumn(c2);

    allColumns.add(c1);
    allColumns.add(c2);
    allTables.add(f1);

    Table f2 = new Table("f2");
    Column c3 = new Column(f2, "c3", "int");
    Column c4 = new Column(f2, "c4", "int");
    f2.addColumn(c3);
    f2.addColumn(c4);

    allColumns.add(c3);
    allColumns.add(c4);
    allTables.add(f2);

    Table f3 = new Table("f3");
    Column c5 = new Column(f3, "c5", "int");
    Column c6 = new Column(f3, "c6", "int");
    f3.addColumn(c5);
    f3.addColumn(c6);

    allColumns.add(c5);
    allColumns.add(c6);
    allTables.add(f3);

    factTables.add(f1);
    factTables.add(f2);
    factTables.add(f3);

    ignoreFactTables.add(f3);

    Table d1 = new Table("d1");
    Column c11 = new Column(d1, "c11", "int");
    Column c12 = new Column(d1, "c12", "int");
    d1.addColumn(c11);
    d1.addColumn(c12);

    allColumns.add(c11);
    allColumns.add(c12);
    allTables.add(d1);

    Table d2 = new Table("d2");
    Column c13 = new Column(d2, "c13", "int");
    Column c14 = new Column(d2, "c14", "int");
    d2.addColumn(c13);
    d2.addColumn(c14);

    allColumns.add(c13);
    allColumns.add(c14);
    allTables.add(d2);
  }

  @Test
  public void joinAndPredicateFinderTest() throws SqlParseException {
    String sql = "SELECT c1,c2 from f1, f3, d1 where f1.c1=d1.c11 and f3.c5=d1.c12 and d1.c12=100";
    SqlParser sqlParser = SqlParser.create(sql);
    SqlNode node = sqlParser.parseQuery();

    Query q = new Query("q1", sql);
    JoinAndPredicateFinder finder =
        new JoinAndPredicateFinder(q, allTables, allColumns, factTables, ignoreFactTables);
    node.accept(finder);

    Set<FactDimensionJoin> factDimensionJoinSet = finder.getFactDimensionJoinSet();

    assertEquals(1, factDimensionJoinSet.size());

    for (FactDimensionJoin factDimensionJoin : factDimensionJoinSet) {
      assertEquals("f1", factDimensionJoin.getFactTable().getName());
      assertEquals("d1", factDimensionJoin.getDimensionTable().getName());
      for (Predicate predicate : factDimensionJoin.getPredicates()) {
        assertEquals("c12", predicate.getColumn().getName());
        assertTrue(predicate instanceof EqualPredicate);
        EqualPredicate eq = (EqualPredicate) predicate;
        assertEquals(100, eq.getValue(), 0.01);
      }
    }
  }

  @Test
  public void aliasExtractor1Test() throws SqlParseException {
    String sql =
        "WITH s1 as (select c1, c2 from t1), s2 as (select c3,c4 from t2) select c1,c4 from s1, s2";
    SqlParser sqlParser = SqlParser.create(sql);
    SqlNode node = sqlParser.parseQuery();

    AliasExtractor extractor = new AliasExtractor();
    node.accept(extractor);

    Map<String, SqlNodeList> map = extractor.getSubQuerySelectListMap();
    assertTrue(map.containsKey("S1"));
    SqlNodeList s1 = map.get("S1");
    SqlIdentifier c1 = (SqlIdentifier) s1.get(0);
    SqlIdentifier c2 = (SqlIdentifier) s1.get(1);

    assertTrue(Utils.equalsLastName(c1, "C1"));
    assertTrue(Utils.equalsLastName(c2, "C2"));

    assertTrue(map.containsKey("S2"));
    SqlNodeList s2 = map.get("S2");
    SqlIdentifier c3 = (SqlIdentifier) s2.get(0);
    SqlIdentifier c4 = (SqlIdentifier) s2.get(1);

    assertTrue(Utils.equalsLastName(c3, "C3"));
    assertTrue(Utils.equalsLastName(c4, "C4"));

    SqlNodeList mainSelectList = extractor.getMainSelectListMap().get(0);

    SqlIdentifier mC1 = (SqlIdentifier) mainSelectList.get(0);
    SqlIdentifier mC4 = (SqlIdentifier) mainSelectList.get(1);

    assertTrue(Utils.equalsLastName(mC1, "C1"));
    assertTrue(Utils.equalsLastName(mC4, "C4"));
  }

  @Test
  public void aliasExtractor2Test() throws SqlParseException {
    String sql =
        "WITH s1 as (select c1, c2 from t1), s2 as (select c3,c4 from t2) "
            + "select c1 from (select c1,c4 from s1, s2)";
    SqlParser sqlParser = SqlParser.create(sql);
    SqlNode node = sqlParser.parseQuery();

    AliasExtractor extractor = new AliasExtractor();
    node.accept(extractor);

    Map<String, SqlNodeList> map = extractor.getSubQuerySelectListMap();
    assertTrue(map.containsKey("S1"));
    SqlNodeList s1 = map.get("S1");
    SqlIdentifier c1 = (SqlIdentifier) s1.get(0);
    SqlIdentifier c2 = (SqlIdentifier) s1.get(1);

    assertTrue(Utils.equalsLastName(c1, "C1"));
    assertTrue(Utils.equalsLastName(c2, "C2"));

    assertTrue(map.containsKey("S2"));
    SqlNodeList s2 = map.get("S2");
    SqlIdentifier c3 = (SqlIdentifier) s2.get(0);
    SqlIdentifier c4 = (SqlIdentifier) s2.get(1);

    assertTrue(Utils.equalsLastName(c3, "C3"));
    assertTrue(Utils.equalsLastName(c4, "C4"));

    SqlNodeList mainSelectList1 = extractor.getMainSelectListMap().get(0);

    SqlIdentifier mC1 = (SqlIdentifier) mainSelectList1.get(0);

    assertEquals(1, mainSelectList1.size());
    assertTrue(Utils.equalsLastName(mC1, "C1"));

    SqlNodeList mainSelectList2 = extractor.getMainSelectListMap().get(1);

    SqlIdentifier m2C1 = (SqlIdentifier) mainSelectList2.get(0);
    SqlIdentifier m2C4 = (SqlIdentifier) mainSelectList2.get(1);

    assertEquals(2, mainSelectList2.size());
    assertTrue(Utils.equalsLastName(m2C1, "C1"));
    assertTrue(Utils.equalsLastName(m2C4, "C4"));
  }

  @Test
  public void aliasExtractor3Test() throws SqlParseException {
    String sql =
        "WITH s1 as (select c1, c2 from t1), s2 as (select c3,c4 from t2) "
            + "select c1 from (select c1,c4 from s1, s2) where c4 < (select avg(val) as v1 from t3)";
    SqlParser sqlParser = SqlParser.create(sql);
    SqlNode node = sqlParser.parseQuery();

    AliasExtractor extractor = new AliasExtractor();
    node.accept(extractor);

    Map<String, SqlNodeList> map = extractor.getSubQuerySelectListMap();
    assertTrue(map.containsKey("S1"));
    SqlNodeList s1 = map.get("S1");
    SqlIdentifier c1 = (SqlIdentifier) s1.get(0);
    SqlIdentifier c2 = (SqlIdentifier) s1.get(1);

    assertTrue(Utils.equalsLastName(c1, "C1"));
    assertTrue(Utils.equalsLastName(c2, "C2"));

    assertTrue(map.containsKey("S2"));
    SqlNodeList s2 = map.get("S2");
    SqlIdentifier c3 = (SqlIdentifier) s2.get(0);
    SqlIdentifier c4 = (SqlIdentifier) s2.get(1);

    assertTrue(Utils.equalsLastName(c3, "C3"));
    assertTrue(Utils.equalsLastName(c4, "C4"));

    SqlNodeList mainSelectList1 = extractor.getMainSelectListMap().get(0);

    SqlIdentifier mC1 = (SqlIdentifier) mainSelectList1.get(0);

    assertEquals(1, mainSelectList1.size());
    assertTrue(Utils.equalsLastName(mC1, "C1"));

    SqlNodeList mainSelectList2 = extractor.getMainSelectListMap().get(1);

    SqlIdentifier m2C1 = (SqlIdentifier) mainSelectList2.get(0);
    SqlIdentifier m2C4 = (SqlIdentifier) mainSelectList2.get(1);

    assertEquals(2, mainSelectList2.size());
    assertTrue(Utils.equalsLastName(m2C1, "C1"));
    assertTrue(Utils.equalsLastName(m2C4, "C4"));
  }
}
