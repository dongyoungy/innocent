package dyoon.innocent;

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;
import java.util.List;

/** Created by Dong Young Yoon on 10/30/18. */
public class AQPInfo {

  private Query q;
  private Sample s;
  private List<Pair<Integer, List<SqlNode>>> expressionList;
  private List<ColumnType> columnTypeList;
  private List<SqlSelect> errorQueries;
  private SqlNode aqpNode;

  public AQPInfo(
      Query q, Sample s, List<Pair<Integer, List<SqlNode>>> expressionList, SqlNode aqpNode) {
    this.q = q;
    this.s = s;
    this.expressionList = expressionList;
    this.aqpNode = aqpNode;
    this.columnTypeList = new ArrayList<>();
    this.errorQueries = new ArrayList<>();
  }

  public boolean isColumnInExpression(String col) {
    for (Pair<Integer, List<SqlNode>> pair : expressionList) {
      for (SqlNode node : pair.getValue()) {
        if (node instanceof SqlIdentifier) {
          SqlIdentifier id = (SqlIdentifier) node;
          if (id.getSimple().toLowerCase().equals(col.toLowerCase())) {
            return true;
          }
        }
      }
    }
    return false;
  }

  public Query getQuery() {
    return q;
  }

  public Sample getSample() {
    return s;
  }

  public SqlNode getAqpNode() {
    return aqpNode;
  }

  public List<Pair<Integer, List<SqlNode>>> getExpressionList() {
    return expressionList;
  }

  public List<ColumnType> getColumnTypeList() {
    return columnTypeList;
  }

  public void setAqpNode(SqlNode aqpNode) {
    this.aqpNode = aqpNode;
  }

  public void setColumnTypeList(List<ColumnType> columnTypeList) {
    this.columnTypeList.clear();
    this.columnTypeList.addAll(columnTypeList);
  }

  public void addErrorQuery(SqlSelect select) {
    errorQueries.add(select);
  }

  public void addErrorQueries(List<SqlSelect> selectList) {
    errorQueries.addAll(selectList);
  }

  public List<SqlSelect> getErrorQueries() {
    return errorQueries;
  }

  public String getAQPResultTableName() {
    return String.format("%s_sample_%s", q.getId(), s.getSampleTableName());
  }
}
