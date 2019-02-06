package dyoon.innocent.query;

import dyoon.innocent.data.Predicate;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlShuttle;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/** Created by Dong Young Yoon on 2019-02-02. */
public class PartitionPredicateReplacer extends SqlShuttle {

  private Set<Predicate> predicatesToReplace;
  private Set<SqlNode> nodeToReplace;
  private Map<SqlNode, Predicate> nodeToPredicate;

  public PartitionPredicateReplacer(Set<Predicate> predicatesToReplace) {
    this.predicatesToReplace = predicatesToReplace;
    this.nodeToPredicate = new HashMap<>();
    this.nodeToReplace = new HashSet<>();
    for (Predicate predicate : predicatesToReplace) {
      nodeToReplace.add(predicate.getCorrespondingNode());
      nodeToPredicate.put(predicate.getCorrespondingNode(), predicate);
    }
  }

  @Override
  public SqlNode visit(SqlCall call) {
    if (call instanceof SqlBasicCall) {
      SqlBasicCall bc = (SqlBasicCall) call;
      if (nodeToReplace.contains(bc)) {
        SqlBasicCall partitionPredicate = (SqlBasicCall) bc.clone(SqlParserPos.ZERO);
        Predicate predicate = nodeToPredicate.get(bc);
        SqlIdentifier toId =
            new SqlIdentifier(predicate.getColumn().getName() + "_part", SqlParserPos.ZERO);
        IdReplacer idReplacer = new IdReplacer(toId);
        partitionPredicate = (SqlBasicCall) partitionPredicate.accept(idReplacer);

        // make 'A = x AND A_part = x'
        return new SqlBasicCall(
            SqlStdOperatorTable.AND, new SqlNode[] {bc, partitionPredicate}, SqlParserPos.ZERO);
      }
    }
    return super.visit(call);
  }
}
