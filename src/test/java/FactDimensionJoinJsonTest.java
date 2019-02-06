import com.fasterxml.jackson.databind.ObjectMapper;
import dyoon.innocent.Query;
import dyoon.innocent.data.Column;
import dyoon.innocent.data.EqualPredicate;
import dyoon.innocent.data.FactDimensionJoin;
import dyoon.innocent.data.PartitionCandidate;
import dyoon.innocent.data.Predicate;
import dyoon.innocent.data.Prejoin;
import dyoon.innocent.data.Table;
import dyoon.innocent.data.UnorderedPair;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/** Created by Dong Young Yoon on 2019-01-31. */
public class FactDimensionJoinJsonTest {

  @BeforeClass
  public static void setup() {}

  @Test
  public void toJsonTest() throws IOException {

    Table factTable = new Table("f1");
    Table dimTable = new Table("d1");

    Column factC1 = new Column(factTable, "f1_c1", "integer");
    Column factC2 = new Column(factTable, "f1_c2", "integer");

    Column dimC1 = new Column(dimTable, "d1_c1", "integer");
    Column dimC2 = new Column(dimTable, "d1_c2", "integer");

    factTable.addColumn(factC1);
    factTable.addColumn(factC2);

    dimTable.addColumn(dimC1);
    dimTable.addColumn(dimC2);

    UnorderedPair<Column> joinColumnPair = new UnorderedPair<>(factC2, dimC2);
    Set<UnorderedPair<Column>> joinColumnSet = new HashSet<>();
    joinColumnSet.add(joinColumnPair);

    Predicate p = new EqualPredicate(dimC1, 100);
    Query q = new Query("q1", "SELECT * FROM f1_c1");

    FactDimensionJoin j = new FactDimensionJoin(factTable, dimTable, joinColumnSet);
    j.addPredicate(p);
    j.addQuery(q);

    Set<FactDimensionJoin> joinSet = new HashSet<>();
    joinSet.add(j);

    Prejoin prejoin = new Prejoin(factTable);
    prejoin.addJoin(j);
    prejoin.addQuery(q);

    PartitionCandidate pc = new PartitionCandidate(prejoin, dimTable.getColumns());

    ObjectMapper mapper = new ObjectMapper();
    String s = mapper.writeValueAsString(pc);

    System.out.println(s);

    PartitionCandidate candidate = mapper.readValue(s, PartitionCandidate.class);

    String partitionTableName = candidate.getPartitionTableName();

    System.out.println(s);
  }
}
