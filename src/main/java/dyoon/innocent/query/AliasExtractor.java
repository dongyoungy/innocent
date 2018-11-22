package dyoon.innocent.query;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlWithItem;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlShuttle;

import java.util.HashMap;
import java.util.Map;

/** Created by Dong Young Yoon on 11/20/18. */
public class AliasExtractor extends SqlShuttle {

  private Map<String, SqlNodeList> subQuerySelectListMap;
  private Map<Integer, SqlNodeList> mainSelectListMap;
  private int selectDepth;

  public AliasExtractor() {
    this.subQuerySelectListMap = new HashMap<>();
    this.mainSelectListMap = new HashMap<>();
    this.selectDepth = 0;
  }

  public Map<String, SqlNodeList> getSubQuerySelectListMap() {
    return subQuerySelectListMap;
  }

  public Map<Integer, SqlNodeList> getMainSelectListMap() {
    return mainSelectListMap;
  }

  @Override
  public SqlNode visit(SqlCall call) {
    if (call instanceof SqlWithItem) {
      SqlWithItem item = (SqlWithItem) call;
      String subqueryAlias = item.name.toString();
      SelectListExtractor extractor = new SelectListExtractor();
      item.query.accept(extractor);
      subQuerySelectListMap.put(subqueryAlias, extractor.getSelectList());
      return call;
    } else if (call instanceof SqlSelect) {
      SqlSelect select = (SqlSelect) call;
      mainSelectListMap.put(selectDepth++, select.getSelectList());
      // here we ignore SELECT in WHERE clause
      select.getFrom().accept(this);
      return select;
    } else {
      return super.visit(call);
    }
  }

  class SelectListExtractor extends SqlShuttle {

    private SqlNodeList selectList;

    public SelectListExtractor() {
      selectList = new SqlNodeList(SqlParserPos.ZERO);
    }

    public SqlNodeList getSelectList() {
      return selectList;
    }

    @Override
    public SqlNode visit(SqlCall call) {
      if (call instanceof SqlSelect) {
        SqlSelect select = (SqlSelect) call;
        for (SqlNode node : select.getSelectList()) {
          selectList.add(node);
        }
        return select;
      } else {
        return super.visit(call);
      }
    }
  }
}
