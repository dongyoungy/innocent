package dyoon.innocent.query;

import com.google.common.collect.HashBasedTable;
import dyoon.innocent.Query;
import dyoon.innocent.Utils;
import dyoon.innocent.data.AliasedTable;
import dyoon.innocent.data.Column;
import dyoon.innocent.data.FactDimensionJoin;
import dyoon.innocent.data.InPredicate;
import dyoon.innocent.data.Join;
import dyoon.innocent.data.Predicate;
import dyoon.innocent.data.RangePredicate;
import dyoon.innocent.data.Table;
import dyoon.innocent.data.UnorderedPair;
import org.apache.calcite.sql.SqlAsOperator;
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

/** Created by Dong Young Yoon on 2018-12-15. */
public class JoinAndPredicateFinder extends SqlShuttle {

  private Query query;
  private Set<Join> joinSet;
  private Set<Table> allTables;
  private List<Column> allColumns;

  private Set<Table> factTableSet;
  private Set<Table> ignoreFactTableSet;

  private Set<FactDimensionJoin> factDimensionJoinSet;

  public JoinAndPredicateFinder(Query query, Set<Table> allTables, List<Column> allColumns) {
    this.query = query;
    this.joinSet = new HashSet<>();
    this.allTables = allTables;
    this.allColumns = allColumns;
    this.factTableSet = new HashSet<>();
    this.ignoreFactTableSet = new HashSet<>();
    this.factDimensionJoinSet = new HashSet<>();
  }

  public JoinAndPredicateFinder(
      Query query,
      Set<Table> allTables,
      List<Column> allColumns,
      Set<Table> factTableSet,
      Set<Table> ignoreFactTableSet) {
    this.query = query;
    this.joinSet = new HashSet<>();
    this.allTables = allTables;
    this.allColumns = allColumns;
    this.factTableSet = factTableSet;
    this.ignoreFactTableSet = ignoreFactTableSet;
    this.factDimensionJoinSet = new HashSet<>();
  }

  public Set<FactDimensionJoin> getFactDimensionJoinSet() {
    return factDimensionJoinSet;
  }

  public Set<Join> getJoinSet() {
    return joinSet;
  }

  @Override
  public SqlNode visit(SqlCall call) {
    if (call instanceof SqlSelect) {
      SqlSelect select = (SqlSelect) call;
      TableExtractor extractor = new TableExtractor();
      if (select.getFrom() != null) {
        select.getFrom().accept(extractor);
      }
      if (select.getWhere() != null) {
        select.getWhere().accept(extractor);
      }
      // joined aliasedTables with alias information
      Set<AliasedTable> tables = extractor.getTables();
      PredicateExtractor pe = new PredicateExtractor(tables);
      if (select.getWhere() != null) {
        select.getWhere().accept(pe);
      }

      query.addTableAll(tables);

      Map<AliasedTable, Set<Predicate>> predicates = pe.getPredicateMap();
      Set<UnorderedPair<SqlIdentifier>> joinKeyPairSet = extractor.getJoinKeySet();
      joinKeyPairSet.addAll(extractor.getJoinKeySet());

      // add predicates to query
      //      query.addPredicateAll(predicates);

      com.google.common.collect.Table<AliasedTable, AliasedTable, Set<UnorderedPair<SqlIdentifier>>>
          joinTable = HashBasedTable.create();
      // gather fact-dimension table join relationship information
      for (UnorderedPair<SqlIdentifier> keyPair : joinKeyPairSet) {
        SqlIdentifier leftKey = keyPair.getLeft();
        SqlIdentifier rightKey = keyPair.getRight();

        AliasedTable leftTable = findAliasedTableForJoinKey(tables, leftKey);
        AliasedTable rightTable = findAliasedTableForJoinKey(tables, rightKey);

        if (leftTable == null || rightTable == null) {
          Logger.error(
              "Table not found for join keys: {} and {}",
              leftKey.getSimple(),
              rightKey.getSimple());
          continue;
        }

        int factTableCount = 0;
        AliasedTable factTable = null, dimTable = null;
        if (isFactTable(leftTable)) {
          factTable = leftTable;
          dimTable = rightTable;
          ++factTableCount;
        }
        if (isFactTable(rightTable)) {
          factTable = rightTable;
          dimTable = leftTable;
          ++factTableCount;
        }

        if (factTableCount == 1) { // i.e., fact + dimension
          if (!joinTable.contains(factTable, dimTable)) {
            joinTable.put(factTable, dimTable, new HashSet<>());
          }
          Set<UnorderedPair<SqlIdentifier>> joinPairSet = joinTable.get(factTable, dimTable);
          joinPairSet.add(keyPair);
        }
      }

      for (com.google.common.collect.Table.Cell<
              AliasedTable, AliasedTable, Set<UnorderedPair<SqlIdentifier>>>
          cell : joinTable.cellSet()) {
        AliasedTable factTable = cell.getRowKey();
        AliasedTable dimTable = cell.getColumnKey();
        Set<UnorderedPair<SqlIdentifier>> joinKeyPairs = cell.getValue();
        Set<UnorderedPair<Column>> joinColumnPairs = Utils.getColumnPairs(allColumns, joinKeyPairs);

        FactDimensionJoin newFactDimJoin =
            FactDimensionJoin.create(factTable, dimTable, joinKeyPairs, allColumns);
        if (predicates.get(factTable) != null) {
          Set<Predicate> predicateSet = predicates.get(factTable);
          newFactDimJoin.addPredicateAll(predicateSet);
          query.addPredicateAll(
              factTable.getTable(), dimTable.getTable(), joinColumnPairs, predicateSet);
        }
        if (predicates.get(dimTable) != null) {
          Set<Predicate> predicateSet = predicates.get(dimTable);
          newFactDimJoin.addPredicateAll(predicateSet);
          query.addPredicateAll(
              factTable.getTable(), dimTable.getTable(), joinColumnPairs, predicateSet);
        }
        factDimensionJoinSet.add(newFactDimJoin);
      }
    }
    return super.visit(call);
  }

  private boolean isFactTable(AliasedTable table) {
    for (Table ignoreFactTable : ignoreFactTableSet) {
      if (ignoreFactTable.getName().equalsIgnoreCase(table.getTable().getName())) {
        return false;
      }
    }
    for (Table factTable : factTableSet) {
      if (factTable.getName().equalsIgnoreCase(table.getTable().getName())) {
        return true;
      }
    }
    return false;
  }

  private AliasedTable findAliasedTableForJoinKey(Set<AliasedTable> tables, SqlIdentifier key) {
    String alias = Utils.getTableAlias(key);
    if (alias == null) {
      return Utils.findAliasedTableContainingColumn(tables, key);
    } else {
      return Utils.findAliasedTableByAlias(tables, alias);
    }
  }

  class TableExtractor extends SqlShuttle {

    private Set<AliasedTable> aliasedTables;
    private Set<UnorderedPair<SqlIdentifier>> joinKeySet;

    public TableExtractor() {
      this.aliasedTables = new HashSet<>();
      this.joinKeySet = new HashSet<>();
    }

    public Set<AliasedTable> getTables() {
      return aliasedTables;
    }

    public Set<UnorderedPair<SqlIdentifier>> getJoinKeySet() {
      return joinKeySet;
    }

    @Override
    public SqlNode visit(SqlIdentifier id) {
      Table t = Utils.findTableByName(allTables, id.names.get(id.names.size() - 1));
      if (t != null) aliasedTables.add(new AliasedTable(t));
      return super.visit(id);
    }

    @Override
    public SqlNode visit(SqlCall call) {
      if (call instanceof SqlBasicCall) {
        SqlBasicCall bc = (SqlBasicCall) call;
        SqlOperator op = bc.getOperator();
        if (op instanceof SqlAsOperator) {
          if (bc.operands[0] instanceof SqlIdentifier && bc.operands[1] instanceof SqlIdentifier) {
            SqlIdentifier tableNameId = (SqlIdentifier) bc.operands[0];
            SqlIdentifier aliasId = (SqlIdentifier) bc.operands[1];
            Table t = Utils.findTableByName(allTables, Utils.getLastName(tableNameId));
            if (t != null) {
              AliasedTable aliasedTable = new AliasedTable(t, Utils.getLastName(aliasId));
              aliasedTables.add(aliasedTable);
            }
          }
        } else if (op instanceof SqlBinaryOperator) {
          String opName = op.getName();
          if (opName.equals("=")) {
            if (bc.operands[0] instanceof SqlIdentifier
                && bc.operands[1] instanceof SqlIdentifier) {
              UnorderedPair<SqlIdentifier> newKeyPair =
                  new UnorderedPair<>(
                      (SqlIdentifier) bc.operands[0], (SqlIdentifier) bc.operands[1]);
              joinKeySet.add(newKeyPair);
            }
          }
        } else {
          return super.visit(call);
        }
      } else {
        return super.visit(call);
      }
      return super.visit(call);
    }
  }

  class PredicateExtractor extends SqlShuttle {

    private Set<AliasedTable> aliasedTables;
    private Map<AliasedTable, Set<Predicate>> predicateMap;

    public PredicateExtractor(Set<AliasedTable> aliasedTables) {
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
          Triple<SqlIdentifier, Double, Boolean> predicate = this.getBinaryPredicate(bc);
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
            Double value = predicate.getMiddle();
            boolean isColumnOnLeft = predicate.getRight();
            switch (opName) {
              case "=":
                {
                  Predicate p = Utils.buildEqualPredicate(allColumns, column, value);
                  if (p != null) this.predicateMap.get(sourceTable).add(p);
                  break;
                }
              case "<":
                {
                  Predicate p =
                      (isColumnOnLeft
                          ? Utils.buildLessThanPredicate(allColumns, column, value)
                          : Utils.buildGreaterThanPredicate(allColumns, column, value));
                  if (p != null) this.predicateMap.get(sourceTable).add(p);
                  break;
                }
              case "<=":
                {
                  Predicate p =
                      (isColumnOnLeft
                          ? Utils.buildLessThanEqualPredicate(allColumns, column, value)
                          : Utils.buildGreaterThanEqualPredicate(allColumns, column, value));
                  if (p != null) this.predicateMap.get(sourceTable).add(p);
                  break;
                }
              case ">":
                {
                  Predicate p =
                      (isColumnOnLeft
                          ? Utils.buildGreaterThanPredicate(allColumns, column, value)
                          : Utils.buildLessThanPredicate(allColumns, column, value));
                  if (p != null) this.predicateMap.get(sourceTable).add(p);
                  break;
                }
              case ">=":
                {
                  Predicate p =
                      (isColumnOnLeft
                          ? Utils.buildGreaterThanEqualPredicate(allColumns, column, value)
                          : Utils.buildLessThanEqualPredicate(allColumns, column, value));
                  if (p != null) this.predicateMap.get(sourceTable).add(p);
                  break;
                }
              default:
                break;
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
    private Triple<SqlIdentifier, Double, Boolean> getBinaryPredicate(SqlBasicCall bc) {
      if (bc.operands.length == 2) {
        if (bc.operands[0] instanceof SqlIdentifier
            && bc.operands[1] instanceof SqlNumericLiteral) {
          SqlIdentifier col = (SqlIdentifier) bc.operands[0];
          SqlNumericLiteral num = (SqlNumericLiteral) bc.operands[1];
          Double val = num.bigDecimalValue().doubleValue();
          return ImmutableTriple.of(col, val, true);
        } else if (bc.operands[1] instanceof SqlIdentifier
            && bc.operands[0] instanceof SqlNumericLiteral) {
          SqlIdentifier col = (SqlIdentifier) bc.operands[1];
          SqlNumericLiteral num = (SqlNumericLiteral) bc.operands[0];
          Double val = num.bigDecimalValue().doubleValue();
          return ImmutableTriple.of(col, val, false);
        }
      }
      return null;
    }
  }
}
