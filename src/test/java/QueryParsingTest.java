import dyoon.innocent.Utils;
import dyoon.innocent.query.AliasExtractor;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** Created by Dong Young Yoon on 11/20/18. */
public class QueryParsingTest {

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
        "WITH s1 as (select c1, c2 from t1), s2 as (select c3,c4 from t2) select c1 from (select c1,c4 from s1, s2)";
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
