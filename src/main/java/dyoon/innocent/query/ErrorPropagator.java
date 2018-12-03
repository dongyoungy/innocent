package dyoon.innocent.query;

import dyoon.innocent.ErrorColumnGenerator;
import dyoon.innocent.Utils;
import org.apache.calcite.sql.SqlAsOperator;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlShuttle;
import org.apache.calcite.util.Litmus;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Created by Dong Young Yoon on 2018-11-27. */
public class ErrorPropagator extends SqlShuttle {

  private Map<SqlIdentifier, SqlIdentifier> exprToErrorMap;
  private Set<SqlNodeList> transformedSelectListSet;

  public ErrorPropagator(
      List<Pair<SqlIdentifier, SqlIdentifier>> exprToErrorMap,
      Set<SqlNodeList> transformedSelectListSet) {
    this.exprToErrorMap = new HashMap<>();
    this.transformedSelectListSet = transformedSelectListSet;
    for (Pair<SqlIdentifier, SqlIdentifier> pair : exprToErrorMap) {
      this.exprToErrorMap.put(pair.getLeft(), pair.getRight());
    }
  }

  public Map<SqlIdentifier, SqlIdentifier> getExprToErrorMap() {
    return exprToErrorMap;
  }

  @Override
  public SqlNode visit(SqlCall call) {
    SqlNode node = super.visit(call);

    if (call instanceof SqlSelect) {
      // for each select, gather all aliases
      SqlSelect select = (SqlSelect) call;
      AliasCollector collector = new AliasCollector();
      SqlNodeList selectList = select.getSelectList();
      selectList.accept(collector);

      List<SqlIdentifier> aliasList = collector.getAliasList();

      if (!transformedSelectListSet.contains(selectList)) {
        List<SqlNode> errors = new ArrayList<>();
        ErrorColumnGenerator errorGen = new ErrorColumnGenerator(exprToErrorMap);
        List<Pair<SqlIdentifier, SqlIdentifier>> newExprErrorPairs = new ArrayList<>();

        for (SqlNode item : selectList) {
          SqlNode err = errorGen.generateErrorColumn(item);
          SqlIdentifier alias = Utils.getAliasIfExists(item);
          if (!err.equalsDeep(item, Litmus.IGNORE)) {
            SqlIdentifier errorAlias =
                new SqlIdentifier(alias.toString() + "_error", SqlParserPos.ZERO);
            Utils.setAliasIfPossible(err, errorAlias);
            errors.add(err);
            newExprErrorPairs.add(ImmutablePair.of(alias, errorAlias));
          }
        }

        for (SqlNode err : errors) {
          selectList.add(err);
        }

        for (Pair<SqlIdentifier, SqlIdentifier> pair : newExprErrorPairs) {
          exprToErrorMap.put(pair.getLeft(), pair.getRight());
        }
      }

      return select;

      // if error alias does not exist for a given expression, we should propagate
      //      for (Map.Entry<SqlIdentifier, SqlIdentifier> entry : exprToErrorMap.entrySet()) {
      //        SqlIdentifier expr = entry.getKey();
      //        SqlIdentifier error = entry.getValue();
      //        if (!aliasList.contains(expr)) {
      //          // select list contains expression
      //          if (!aliasList.contains(error)) {
      //            // but does not have error -> we should propagate
      //            List<SqlNode> errors = new ArrayList<>();
      //            SqlNodeList selectList = select.getSelectList();
      //            ErrorColumnGenerator errorGen = new ErrorColumnGenerator(exprToErrorMap);
      //            for (SqlNode item : selectList) {
      //              SqlNode err = errorGen.generateErrorColumn(item);
      //              if (!err.equalsDeep(item, Litmus.IGNORE)) {
      //                errors.add(err);
      //              }
      //            }
      //
      //            for (SqlNode err : errors) {
      //              selectList.add(err);
      //            }
      //          }
      //        }
      //      }
    }
    return node;
  }

  class AliasCollector extends SqlShuttle {

    private List<SqlIdentifier> aliasList;

    public AliasCollector() {
      this.aliasList = new ArrayList<>();
    }

    public List<SqlIdentifier> getAliasList() {
      return aliasList;
    }

    @Override
    public SqlNode visit(SqlCall call) {
      if (call instanceof SqlBasicCall) {
        SqlBasicCall bc = (SqlBasicCall) call;
        SqlOperator op = bc.getOperator();
        if (op instanceof SqlAsOperator) {
          aliasList.add((SqlIdentifier) bc.operands[1]);
        }
      }
      return super.visit(call);
    }
  }
}
