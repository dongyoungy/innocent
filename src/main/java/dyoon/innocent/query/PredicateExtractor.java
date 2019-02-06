package dyoon.innocent.query;

import dyoon.innocent.Utils;
import dyoon.innocent.data.AliasedTable;
import dyoon.innocent.data.Column;
import dyoon.innocent.data.InPredicate;
import dyoon.innocent.data.Predicate;
import dyoon.innocent.data.RangePredicate;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlBinaryOperator;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.fun.SqlBetweenOperator;
import org.apache.calcite.sql.fun.SqlInOperator;
import org.apache.calcite.sql.util.SqlShuttle;
import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.apache.commons.lang3.tuple.Triple;
import org.pmw.tinylog.Logger;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Created by Dong Young Yoon on 2019-02-01. */
public class PredicateExtractor extends SqlShuttle {

  private List<Column> allColumns;
  private Set<AliasedTable> aliasedTables;
  private Map<AliasedTable, Set<Predicate>> predicateMap;

  public PredicateExtractor(List<Column> allColumns, Set<AliasedTable> aliasedTables) {
    this.allColumns = allColumns;
    this.aliasedTables = aliasedTables;
    this.predicateMap = new HashMap<>();
  }

  public Map<AliasedTable, Set<Predicate>> getPredicateMap() {
    return predicateMap;
  }

  @Override
  public SqlNode visit(SqlCall call) {
    if (call instanceof SqlSelect) {
      // ignore select in WHERE
      return call;
    } else if (call instanceof SqlBasicCall) {
      SqlBasicCall bc = (SqlBasicCall) call;
      SqlOperator op = bc.getOperator();
      if (op instanceof SqlBinaryOperator) {
        String opName = op.getName();
        Triple<SqlIdentifier, Object, Boolean> predicate = this.getBinaryPredicate(bc);
        if (predicate != null) {
          SqlIdentifier column = predicate.getLeft();
          AliasedTable sourceTable = getSourceTable(column);
          if (sourceTable == null) {
            Logger.error("Could not find source table for column: {}", column.getSimple());
            return call;
          }
          if (!predicateMap.containsKey(sourceTable)) {
            predicateMap.put(sourceTable, new HashSet<>());
          }
          Object value = predicate.getMiddle();
          boolean isColumnOnLeft = predicate.getRight();
          Predicate p = null;
          switch (opName) {
            case "=":
              {
                p = Utils.buildEqualPredicate(allColumns, column, value);
                break;
              }
            case "<":
              {
                p =
                    (isColumnOnLeft
                        ? Utils.buildLessThanPredicate(allColumns, column, value)
                        : Utils.buildGreaterThanPredicate(allColumns, column, value));
                break;
              }
            case "<=":
              {
                p =
                    (isColumnOnLeft
                        ? Utils.buildLessThanEqualPredicate(allColumns, column, value)
                        : Utils.buildGreaterThanEqualPredicate(allColumns, column, value));
                break;
              }
            case ">":
              {
                p =
                    (isColumnOnLeft
                        ? Utils.buildGreaterThanPredicate(allColumns, column, value)
                        : Utils.buildLessThanPredicate(allColumns, column, value));
                break;
              }
            case ">=":
              {
                p =
                    (isColumnOnLeft
                        ? Utils.buildGreaterThanEqualPredicate(allColumns, column, value)
                        : Utils.buildLessThanEqualPredicate(allColumns, column, value));
                break;
              }
            default:
              break;
          }
          if (p != null) {
            p.setCorrespondingNode(call);
            this.predicateMap.get(sourceTable).add(p);
          }
        } else if (op instanceof SqlInOperator) {
          SqlNode left = bc.operands[0];
          SqlNode right = bc.operands[1];
          if (left instanceof SqlIdentifier && right instanceof SqlNodeList) {
            SqlIdentifier colId = (SqlIdentifier) left;
            SqlNodeList values = (SqlNodeList) right;
            AliasedTable sourceTable = getSourceTable(colId);
            if (sourceTable == null) {
              Logger.error("Could not find source table for column: {}", colId.getSimple());
              return call;
            }
            if (!predicateMap.containsKey(sourceTable)) {
              predicateMap.put(sourceTable, new HashSet<>());
            }
            Column column = Utils.findColumnById(allColumns, colId);
            InPredicate p = new InPredicate(column);
            for (SqlNode value : values) {
              if (value instanceof SqlLiteral) {
                SqlLiteral val = (SqlLiteral) value;
                p.addValue(val.toValue());
              }
            }
            p.setCorrespondingNode(call);
            this.predicateMap.get(sourceTable).add(p);
          }
        } else if (op instanceof SqlBetweenOperator) {
          SqlNode n1 = bc.operands[0];
          SqlNode n2 = bc.operands[1];
          SqlNode n3 = bc.operands[2];
          if (n1 instanceof SqlIdentifier
              && n2 instanceof SqlNumericLiteral
              && n3 instanceof SqlNumericLiteral) {
            SqlIdentifier colId = (SqlIdentifier) n1;
            SqlNumericLiteral lb = (SqlNumericLiteral) n2;
            SqlNumericLiteral ub = (SqlNumericLiteral) n3;
            AliasedTable sourceTable = getSourceTable(colId);
            if (sourceTable == null) {
              Logger.error("Could not find source table for column: {}", colId.getSimple());
              return call;
            }
            if (!predicateMap.containsKey(sourceTable)) {
              predicateMap.put(sourceTable, new HashSet<>());
            }
            Column column = Utils.findColumnById(allColumns, colId);
            RangePredicate p =
                new RangePredicate(
                    column,
                    lb.bigDecimalValue().doubleValue(),
                    ub.bigDecimalValue().doubleValue(),
                    true,
                    true);
            p.setCorrespondingNode(call);
            this.predicateMap.get(sourceTable).add(p);
          }
        }
      }
    }
    return super.visit(call);
  }

  private AliasedTable getSourceTable(SqlIdentifier column) {

    String alias = Utils.getTableAlias(column);
    AliasedTable table = Utils.findAliasedTableContainingColumn(aliasedTables, column);

    if (table == null) return null;

    if (alias != null) {
      if (table.getAlias() != null) {
        if (table.getAlias().equalsIgnoreCase(alias)) {
          return table;
        }
      } else {
        if (table.getTable().getName().equalsIgnoreCase(alias)) {
          return table;
        }
      }
    } else {
      return table;
    }

    return null;
  }

  // returns {column, value, isColumnOnLeft} for binary predicate
  private Triple<SqlIdentifier, Object, Boolean> getBinaryPredicate(SqlBasicCall bc) {
    if (bc.operands.length == 2) {
      if (bc.operands[0] instanceof SqlIdentifier && bc.operands[1] instanceof SqlLiteral) {
        SqlIdentifier col = (SqlIdentifier) bc.operands[0];
        SqlLiteral val = (SqlLiteral) bc.operands[1];
        String strValue = val.toString();
        return ImmutableTriple.of(col, strValue, true);
      } else if (bc.operands[1] instanceof SqlIdentifier
          && bc.operands[0] instanceof SqlNumericLiteral) {
        SqlIdentifier col = (SqlIdentifier) bc.operands[1];
        SqlLiteral val = (SqlLiteral) bc.operands[0];
        String strValue = val.toString();
        return ImmutableTriple.of(col, val, false);
      }
    }
    return null;
  }
}
