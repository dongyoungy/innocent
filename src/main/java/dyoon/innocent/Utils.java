package dyoon.innocent;

import dyoon.innocent.data.AliasedTable;
import dyoon.innocent.data.Column;
import dyoon.innocent.data.EqualPredicate;
import dyoon.innocent.data.Predicate;
import dyoon.innocent.data.RangePredicate;
import dyoon.innocent.data.Table;
import dyoon.innocent.data.UnorderedPair;
import org.apache.calcite.sql.SqlAsOperator;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.Litmus;
import org.pmw.tinylog.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** Created by Dong Young Yoon on 11/4/18. */
public class Utils {
  public static boolean containsIgnoreCase(String str, List<String> list) {
    for (String s : list) {
      if (s.equalsIgnoreCase(str)) return true;
    }
    return false;
  }

  public static boolean containsNode(SqlNode node, SqlNodeList list) {
    for (SqlNode n : list) {
      if (n.equalsDeep(node, Litmus.IGNORE)) {
        return true;
      }
    }
    return false;
  }

  public static SqlIdentifier getAliasIfExists(SqlNode node) {
    if (node instanceof SqlBasicCall) {
      SqlBasicCall bc = (SqlBasicCall) node;
      SqlOperator op = bc.getOperator();
      if (op instanceof SqlAsOperator) {
        return (SqlIdentifier) bc.operands[1];
      }
    }
    return null;
  }

  public static void setAliasIfPossible(SqlNode node, SqlIdentifier alias) {
    if (node instanceof SqlBasicCall) {
      SqlBasicCall bc = (SqlBasicCall) node;
      SqlOperator op = bc.getOperator();
      if (op instanceof SqlAsOperator) {
        bc.setOperand(1, alias);
      }
    }
  }

  public static SqlNode stripAliasIfExists(SqlNode node) {
    if (node instanceof SqlBasicCall) {
      SqlBasicCall bc = (SqlBasicCall) node;
      SqlOperator op = bc.getOperator();
      if (op instanceof SqlAsOperator) {
        return bc.operands[0];
      }
    }
    return node;
  }

  public static SqlNode constructScaledAgg(SqlNode source, SqlIdentifier statTable) {
    SqlNode c = SqlLiteral.createExactNumeric("100000", SqlParserPos.ZERO);

    SqlBasicCall scaled =
        new SqlBasicCall(
            SqlStdOperatorTable.MULTIPLY, new SqlNode[] {source, c}, SqlParserPos.ZERO);
    scaled =
        new SqlBasicCall(
            SqlStdOperatorTable.DIVIDE,
            new SqlNode[] {scaled, statTable.plus("groupsize", SqlParserPos.ZERO)},
            SqlParserPos.ZERO);
    scaled =
        new SqlBasicCall(
            SqlStdOperatorTable.MULTIPLY,
            new SqlNode[] {scaled, statTable.plus("actualsize", SqlParserPos.ZERO)},
            SqlParserPos.ZERO);
    scaled =
        new SqlBasicCall(SqlStdOperatorTable.DIVIDE, new SqlNode[] {scaled, c}, SqlParserPos.ZERO);

    return scaled;
  }

  public static SqlBasicCall alias(SqlNode source, SqlIdentifier alias) {
    return new SqlBasicCall(
        SqlStdOperatorTable.AS, new SqlNode[] {source, alias}, SqlParserPos.ZERO);
  }

  public static SqlBasicCall plus(SqlNode left, SqlNode right) {
    return new SqlBasicCall(
        SqlStdOperatorTable.PLUS, new SqlNode[] {left, right}, SqlParserPos.ZERO);
  }

  public static SqlBasicCall multiply(SqlNode left, SqlNode right) {
    return new SqlBasicCall(
        SqlStdOperatorTable.MULTIPLY, new SqlNode[] {left, right}, SqlParserPos.ZERO);
  }

  public static SqlBasicCall divide(SqlNode left, SqlNode right) {
    return new SqlBasicCall(
        SqlStdOperatorTable.DIVIDE, new SqlNode[] {left, right}, SqlParserPos.ZERO);
  }

  public static SqlBasicCall sum(SqlNode source) {
    return new SqlBasicCall(SqlStdOperatorTable.SUM, new SqlNode[] {source}, SqlParserPos.ZERO);
  }

  public static SqlBasicCall avg(SqlNode source) {
    return new SqlBasicCall(SqlStdOperatorTable.AVG, new SqlNode[] {source}, SqlParserPos.ZERO);
  }

  public static SqlBasicCall count(SqlNode source) {
    return new SqlBasicCall(SqlStdOperatorTable.COUNT, new SqlNode[] {source}, SqlParserPos.ZERO);
  }

  public static SqlBasicCall sqrt(SqlNode source) {
    return new SqlBasicCall(SqlStdOperatorTable.SQRT, new SqlNode[] {source}, SqlParserPos.ZERO);
  }

  public static SqlBasicCall pow(SqlNode source, int e) {
    return new SqlBasicCall(
        SqlStdOperatorTable.POWER,
        new SqlNode[] {
          source, SqlLiteral.createExactNumeric(String.format("%d", e), SqlParserPos.ZERO)
        },
        SqlParserPos.ZERO);
  }

  public static String getLastName(SqlIdentifier id) {
    return id.names.get(id.names.size() - 1);
  }

  public static String getTableAlias(SqlIdentifier id) {
    if (id.names.size() > 1) {
      return id.names.get(id.names.size() - 2);
    }
    return null;
  }

  public static boolean equalsLastName(SqlIdentifier id, String str) {
    String lastName = id.names.get(id.names.size() - 1);
    return lastName.equals(str);
  }

  public static SqlIdentifier matchLastName(Collection<SqlIdentifier> aliasList, SqlIdentifier id) {
    for (SqlIdentifier alias : aliasList) {
      String aliasLastName = alias.names.get(alias.names.size() - 1);
      String idLastName = id.names.get(id.names.size() - 1);

      if (idLastName.equals(aliasLastName)) {
        return alias;
      }
    }
    return null;
  }

  public static boolean containsLastName(Collection<SqlIdentifier> aliasList, SqlIdentifier id) {
    for (SqlIdentifier alias : aliasList) {
      String aliasLastName = alias.names.get(alias.names.size() - 1);
      String idLastName = id.names.get(id.names.size() - 1);

      if (idLastName.equals(aliasLastName)) {
        return true;
      }
    }
    return false;
  }

  public static Table findTableByName(Set<Table> tables, String name) {
    for (Table table : tables) {
      if (table.getName().equalsIgnoreCase(name)) {
        return table;
      }
    }
    return null;
  }

  public static int containsTableAny(Set<Table> tables, String[] names) {
    int count = 0;
    for (Table table : tables) {
      for (String name : names) {
        if (table.getName().equalsIgnoreCase(name)) {
          ++count;
        }
      }
    }
    return count;
  }

  public static Set<Table> getTableSetWithNames(Set<Table> tables, String[] names) {
    Set<Table> tableSet = new HashSet<>();
    for (Table table : tables) {
      for (String name : names) {
        if (table.getName().equalsIgnoreCase(name)) {
          tableSet.add(table);
          break;
        }
      }
    }
    return tableSet;
  }

  public static Column findColumnById(Collection<Column> columns, SqlIdentifier id) {
    String colName = id.names.get(id.names.size() - 1);
    for (Column column : columns) {
      if (column.getName().equalsIgnoreCase(colName)) {
        return column;
      }
    }
    return null;
  }

  public static AliasedTable findAliasedTableByAlias(Set<AliasedTable> tables, String alias) {
    if (!alias.isEmpty()) {
      for (AliasedTable table : tables) {
        if (table.getAlias() != null) {
          if (table.getAlias().equalsIgnoreCase(alias)) {
            return table;
          }
        } else {
          if (table.getTable().getName().equalsIgnoreCase(alias)) {
            return table;
          }
        }
      }
    }
    return null;
  }

  public static AliasedTable findAliasedTableContainingColumn(
      Set<AliasedTable> tables, SqlIdentifier col) {
    String colName = col.names.get(col.names.size() - 1);
    String alias = Utils.getTableAlias(col);

    AliasedTable tableWithAlias = null;
    for (AliasedTable table : tables) {
      if (table.getAlias() != null && table.getAlias().equalsIgnoreCase(alias)) {
        tableWithAlias = table;
        break;
      }
    }

    if (tableWithAlias != null) {
      for (Column column : tableWithAlias.getTable().getColumns()) {
        if (column.getName().equalsIgnoreCase(colName)) {
          return tableWithAlias;
        }
      }
    } else {
      for (AliasedTable table : tables) {
        for (Column column : table.getTable().getColumns()) {
          if (column.getName().equalsIgnoreCase(colName)) {
            return table;
          }
        }
      }
    }

    return null;
  }

  public static Set<UnorderedPair<Column>> getColumnPairs(
      Collection<Column> allColumns, Set<UnorderedPair<SqlIdentifier>> pairSet) {
    Set<UnorderedPair<Column>> columnSet = new HashSet<>();
    for (UnorderedPair<SqlIdentifier> pair : pairSet) {
      Column leftColumn = Utils.findColumnById(allColumns, pair.getLeft());
      Column rightColumn = Utils.findColumnById(allColumns, pair.getRight());

      columnSet.add(new UnorderedPair<>(leftColumn, rightColumn));
    }
    return columnSet;
  }

  public static Table findTableContainingColumn(Set<Table> tables, SqlIdentifier col) {
    String colName = col.names.get(col.names.size() - 1);
    for (Table table : tables) {
      for (Column column : table.getColumns()) {
        if (column.getName().equalsIgnoreCase(colName)) {
          return table;
        }
      }
    }
    return null;
  }

  public static List<Column> getAllColumns(Set<Table> tables) {
    List<Column> allColumns = new ArrayList<>();
    for (Table table : tables) {
      allColumns.addAll(table.getColumns());
    }
    return allColumns;
  }

  public static Predicate buildLessThanPredicate(
      List<Column> allColumns, SqlIdentifier id, Double val) {
    Column col = Utils.findColumnById(allColumns, id);
    if (col == null) {
      Logger.warn("Column not found with id = {}", id.getSimple());
      return null;
    }
    return new RangePredicate(col, Double.NEGATIVE_INFINITY, val, false, false);
  }

  public static Predicate buildLessThanEqualPredicate(
      List<Column> allColumns, SqlIdentifier id, Double val) {
    Column col = Utils.findColumnById(allColumns, id);
    if (col == null) {
      Logger.warn("Column not found with id = {}", id.getSimple());
      return null;
    }
    return new RangePredicate(col, Double.NEGATIVE_INFINITY, val, false, true);
  }

  public static Predicate buildGreaterThanPredicate(
      List<Column> allColumns, SqlIdentifier id, Double val) {
    Column col = Utils.findColumnById(allColumns, id);
    if (col == null) {
      Logger.warn("Column not found with id = {}", id.getSimple());
      return null;
    }
    return new RangePredicate(col, val, Double.POSITIVE_INFINITY, false, false);
  }

  public static Predicate buildGreaterThanEqualPredicate(
      List<Column> allColumns, SqlIdentifier id, Double val) {
    Column col = Utils.findColumnById(allColumns, id);
    if (col == null) {
      Logger.warn("Column not found with id = {}", id.getSimple());
      return null;
    }
    return new RangePredicate(col, val, Double.POSITIVE_INFINITY, true, false);
  }

  public static Predicate buildEqualPredicate(
      List<Column> allColumns, SqlIdentifier id, Double val) {
    Column col = Utils.findColumnById(allColumns, id);
    if (col == null) {
      Logger.warn("Column not found with id = {}", id.getSimple());
      return null;
    }
    return new EqualPredicate(col, val);
  }

  public static RangePredicate buildLessThanPredicate(Column col, Double val) {
    return new RangePredicate(col, Double.NEGATIVE_INFINITY, val, false, false);
  }

  public static RangePredicate buildLessThanEqualPredicate(Column col, Double val) {
    return new RangePredicate(col, Double.NEGATIVE_INFINITY, val, false, true);
  }

  public static RangePredicate buildGreaterThanPredicate(Column col, Double val) {
    return new RangePredicate(col, val, Double.POSITIVE_INFINITY, false, false);
  }

  public static RangePredicate buildGreaterThanEqualPredicate(Column col, Double val) {
    return new RangePredicate(col, val, Double.POSITIVE_INFINITY, true, false);
  }

  public static EqualPredicate buildEqualPredicate(Column col, Double val) {
    return new EqualPredicate(col, val);
  }

  public static RangePredicate buildRangePredicate(Column col, Double val1, Double val2) {
    return new RangePredicate(col, val1, val2, false, false);
  }
}
