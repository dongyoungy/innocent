package dyoon.innocent;

import org.apache.calcite.sql.SqlNode;
import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;
import java.util.List;

/** Created by Dong Young Yoon on 10/30/18. */
public class AQPInfo {

  private Query q;
  private Sample s;
  private List<Pair<Integer, List<SqlNode>>> expressionList;
  private List<ColumnType> columnTypeList;
  private SqlNode aqpNode;

  public AQPInfo(
      Query q, Sample s, List<Pair<Integer, List<SqlNode>>> expressionList, SqlNode aqpNode) {
    this.q = q;
    this.s = s;
    this.expressionList = expressionList;
    this.aqpNode = aqpNode;
    this.columnTypeList = new ArrayList<>();
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

  public void setColumnTypeList(List<ColumnType> columnTypeList) {
    this.columnTypeList.clear();
    this.columnTypeList.addAll(columnTypeList);
  }

  public String getAQPResultTableName() {
    return String.format("%s_sample_%s", q.getId(), s.getSampleTableName());
  }
}
