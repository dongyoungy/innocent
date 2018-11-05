package dyoon.innocent.query;

import dyoon.innocent.Utils;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlWith;
import org.apache.calcite.sql.SqlWithItem;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlShuttle;
import org.apache.calcite.util.Litmus;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by Dong Young Yoon on 10/31/18.
 *
 * <p>Propagates error columns from inner queries to outer query
 */
public class ErrorColumnPropagator extends SqlShuttle {

  private Map<SqlIdentifier, List<String>> errorTableToColumnMap;

  public ErrorColumnPropagator() {
    this.errorTableToColumnMap = new HashMap<>();
  }

  @Override
  public SqlNode visit(SqlCall call) {

    Set<String> added = new HashSet<>();

    if (call instanceof SqlWith) {
      SqlWith with = (SqlWith) call;

      SqlSelect select = (SqlSelect) with.body;
      with.body = this.visit(select);

      // Gather error columns
      ErrorColumnCollector columnCollector = new ErrorColumnCollector();
      for (SqlNode node : with.withList) {
        node.accept(columnCollector);
      }

      with.withList.accept(columnCollector);
      List<String> errorColumns = columnCollector.getErrorColumns();
      this.errorTableToColumnMap.putAll(columnCollector.errorTableToColumnMap);

      // check with table first
      for (SqlIdentifier table : this.errorTableToColumnMap.keySet()) {
        ErrorTableChecker tableChecker = new ErrorTableChecker(table);
        select.accept(tableChecker);

        if (tableChecker.hasErrorTable()) {
          if (!checkSelectStar(select.getSelectList())) {
            List<String> columns = this.errorTableToColumnMap.get(table);
            for (String column : columns) {
              if (!added.contains(column)) {

                ColumnChecker check = new ColumnChecker(column);
                select.getSelectList().accept(check);

                if (!check.hasColumn()) {
                  select.getSelectList().add(new SqlIdentifier(column, SqlParserPos.ZERO));
                  added.add(column);
                }
              }
            }
          }
        }
      }

      for (String column : errorColumns) {
        boolean hasColumn = false;
        for (SqlNode node : select.getSelectList()) {
          ColumnChecker checker = new ColumnChecker(column);
          node.accept(checker);
          if (checker.hasColumn()) {
            hasColumn = true;
            break;
          }
        }
        if (!hasColumn) {
          // if select *, we do not need to propagate error columns
          if (!checkSelectStar(select.getSelectList())) {
            if (!added.contains(column)) {

              ColumnChecker check = new ColumnChecker(column);
              select.getSelectList().accept(check);

              if (!check.hasColumn()) {
                SqlIdentifier errId = new SqlIdentifier(column, SqlParserPos.ZERO);
                SqlNode errCol = new SqlIdentifier(column, SqlParserPos.ZERO);
                if (select.getGroup() != null) errCol = Utils.alias(Utils.avg(errCol), errId);
                select.getSelectList().add(errCol);
                added.add(column);
              }
            }
          }
        }
      }
    } else if (call instanceof SqlSelect) {
      SqlSelect select = (SqlSelect) call;

      // Gather error columns
      ErrorColumnCollector columnCollector = new ErrorColumnCollector();
      call.accept(columnCollector);

      List<String> errorColumns = columnCollector.getErrorColumns();
      this.errorTableToColumnMap.putAll(columnCollector.errorTableToColumnMap);

      // check with table first
      for (SqlIdentifier table : this.errorTableToColumnMap.keySet()) {
        ErrorTableChecker tableChecker = new ErrorTableChecker(table);
        select.accept(tableChecker);

        if (tableChecker.hasErrorTable()) {
          if (!checkSelectStar(select.getSelectList())) {
            List<String> columns = this.errorTableToColumnMap.get(table);
            for (String column : columns) {
              if (!added.contains(column)) {
                ColumnChecker check = new ColumnChecker(column);
                select.getSelectList().accept(check);
                if (!check.hasColumn()) {
                  select.getSelectList().add(new SqlIdentifier(column, SqlParserPos.ZERO));
                  added.add(column);
                }
              }
            }
          }
        }
      }

      for (String column : errorColumns) {
        boolean hasColumn = false;
        for (SqlNode node : select.getSelectList()) {
          ColumnChecker checker = new ColumnChecker(column);
          node.accept(checker);
          if (checker.hasColumn()) {
            hasColumn = true;
            break;
          }
        }
        if (!hasColumn) {
          // if select *, we do not need to propagate error columns
          if (!checkSelectStar(select.getSelectList())) {
            ColumnChecker check = new ColumnChecker(column);
            select.getSelectList().accept(check);
            if (!check.hasColumn()) {
              SqlIdentifier errId = new SqlIdentifier(column, SqlParserPos.ZERO);
              SqlNode errCol = new SqlIdentifier(column, SqlParserPos.ZERO);
              if (select.getGroup() != null) errCol = Utils.alias(Utils.avg(errCol), errId);
              select.getSelectList().add(errCol);
            }
          }
        }
      }
    }
    return super.visit(call);
  }

  private boolean checkSelectStar(SqlNodeList selectList) {
    if (selectList.size() == 1) {
      SqlNode node = selectList.get(0);
      if (node instanceof SqlIdentifier) {
        SqlIdentifier id = (SqlIdentifier) node;
        if (id.names.size() == 1 && id.names.get(0).isEmpty()) {
          return true;
        }
      }
    }
    return false;
  }

  class ColumnChecker extends SqlShuttle {
    String column;
    boolean hasColumn;

    public ColumnChecker(String column) {
      this.column = column;
      this.hasColumn = false;
    }

    public boolean hasColumn() {
      return hasColumn;
    }

    @Override
    public SqlNode visit(SqlIdentifier id) {
      String name = id.names.get(id.names.size() - 1);
      if (name.equalsIgnoreCase(column)) {
        hasColumn = true;
      }
      return super.visit(id);
    }
  }

  class ErrorTableChecker extends SqlShuttle {
    SqlIdentifier errorTable;
    boolean hasErrorTable;

    public ErrorTableChecker(SqlIdentifier errorTable) {
      this.errorTable = errorTable;
      this.hasErrorTable = false;
    }

    public boolean hasErrorTable() {
      return hasErrorTable;
    }

    @Override
    public SqlNode visit(SqlIdentifier id) {
      if (id.equalsDeep(errorTable, Litmus.IGNORE)) {
        hasErrorTable = true;
      }
      return super.visit(id);
    }
  }

  class ErrorColumnCollector extends SqlShuttle {

    private SqlIdentifier currentTable;
    private List<String> columns;
    private Map<SqlIdentifier, List<String>> errorTableToColumnMap;

    public ErrorColumnCollector() {
      columns = new ArrayList<>();
      errorTableToColumnMap = new HashMap<>();
    }

    public List<String> getErrorColumns() {
      return columns;
    }

    @Override
    public SqlNode visit(SqlCall call) {
      if (call instanceof SqlWithItem) {
        SqlWithItem item = (SqlWithItem) call;
        errorTableToColumnMap.put(item.name, new ArrayList<>());
        currentTable = item.name;
      }
      return super.visit(call);
    }

    @Override
    public SqlNode visit(SqlIdentifier id) {
      String name = id.names.get(id.names.size() - 1);
      if (name.toLowerCase().contains("_error")) {
        columns.add(name);
        List<String> list = this.errorTableToColumnMap.get(this.currentTable);
        if (list != null) list.add(name);
      }
      return super.visit(id);
    }
  }
}
