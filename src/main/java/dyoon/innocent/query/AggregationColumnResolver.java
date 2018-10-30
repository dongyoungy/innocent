package dyoon.innocent.query;

import org.apache.calcite.sql.SqlAsOperator;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.util.SqlShuttle;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;
import java.util.List;

/** Created by Dong Young Yoon on 10/30/18. */
public class AggregationColumnResolver extends SqlShuttle {

  private List<SqlIdentifier> aliasList;
  private List<Pair<Integer, List<SqlNode>>> expressionList;

  public AggregationColumnResolver(List<SqlIdentifier> aliasList) {
    this.aliasList = new ArrayList<>(aliasList);
    this.expressionList = new ArrayList<>();
  }

  public List<SqlIdentifier> getAliasList() {
    return aliasList;
  }

  public List<Pair<Integer, List<SqlNode>>> getExpressionList() {
    return expressionList;
  }

  @Override
  public SqlNode visit(SqlCall call) {
    if (call instanceof SqlSelect) {
      SqlSelect select = (SqlSelect) call;

      //      for (SqlNode node : select.getSelectList()) {
      for (int i = 0; i < select.getSelectList().size(); ++i) {
        SqlNode node = select.getSelectList().get(i);
        SelectItemResolver resolver = new SelectItemResolver(aliasList);
        node.accept(resolver);
        if (!resolver.getExpressionList().isEmpty()) {
          this.expressionList.add(ImmutablePair.of(i, resolver.getExpressionList()));
        }
      }
    }
    return super.visit(call);
  }

  class SelectItemResolver extends SqlShuttle {

    private SqlIdentifier currentAlias;
    private boolean containsCurrentAlias;
    private List<SqlIdentifier> aliasList;
    private List<SqlNode> expressionList;

    public SelectItemResolver(List<SqlIdentifier> aliasList) {
      this.aliasList = aliasList;
      this.expressionList = new ArrayList<>();
      this.currentAlias = null;
      this.containsCurrentAlias = false;
    }

    public List<SqlIdentifier> getAliasList() {
      return aliasList;
    }

    public List<SqlNode> getExpressionList() {
      return expressionList;
    }

    @Override
    public SqlNode visit(SqlIdentifier id) {
      String name = id.names.get(id.names.size() - 1);
      for (SqlIdentifier alias : aliasList) {
        String aliasName = alias.names.get(alias.names.size() - 1);
        if (aliasName.equals(name)) {
          this.containsCurrentAlias = true;
        }
      }
      return super.visit(id);
    }

    private boolean contains(SqlIdentifier id) {
      String name = id.names.get(id.names.size() - 1);
      for (SqlIdentifier alias : aliasList) {
        String aliasName = alias.names.get(alias.names.size() - 1);
        if (aliasName.equals(name)) {
          return true;
        }
      }
      return false;
    }

    @Override
    public SqlNode visit(SqlCall call) {
      if (call instanceof SqlBasicCall) {
        SqlBasicCall bc = (SqlBasicCall) call;
        SqlOperator op = bc.getOperator();
        if (op instanceof SqlAsOperator) {
          this.currentAlias = (SqlIdentifier) bc.operands[1];
          this.containsCurrentAlias = false;
          bc.operands[0].accept(SelectItemResolver.this);
          if (this.containsCurrentAlias) {
            aliasList.add(this.currentAlias);
            expressionList.add(bc.operands[0]);
            this.containsCurrentAlias = false;
          }
          if (this.contains(currentAlias)) {
            expressionList.add(this.currentAlias);
          }
          return bc;
        } else {
          return super.visit(call);
        }
      } else {
        return super.visit(call);
      }
    }
  }
}
