package dyoon.innocent.query;

import com.google.common.collect.HashBasedTable;
import dyoon.innocent.Query;
import dyoon.innocent.Utils;
import dyoon.innocent.data.AliasedTable;
import dyoon.innocent.data.Column;
import dyoon.innocent.data.FactDimensionJoin;
import dyoon.innocent.data.Join;
import dyoon.innocent.data.Predicate;
import dyoon.innocent.data.Table;
import dyoon.innocent.data.UnorderedPair;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.util.SqlShuttle;
import org.pmw.tinylog.Logger;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Created by Dong Young Yoon on 2018-12-15. */
public class JoinAndPredicateFinder extends SqlShuttle {

  private Query query;
  private Set<Table> allTables;
  private List<Column> allColumns;

  private Set<Join> joinSet;
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
      super.visit(select); // process current select for its possible inner select first?

      TableExtractor extractor = new TableExtractor(allTables);
      if (select.getFrom() != null) {
        select.getFrom().accept(extractor);
      }
      if (select.getWhere() != null) {
        select.getWhere().accept(extractor);
      }
      // joined aliasedTables with alias information
      Set<AliasedTable> tables = extractor.getTables();
      PredicateExtractor pe = new PredicateExtractor(allColumns, tables);
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

        if (factTableCount == 1 && !isIgnoreFactTable(factTable)) { // i.e., fact + dimension
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
      return select;
    }
    return super.visit(call);
  }

  private boolean isFactTable(AliasedTable table) {
    for (Table factTable : factTableSet) {
      if (factTable.getName().equalsIgnoreCase(table.getTable().getName())) {
        return true;
      }
    }
    return false;
  }

  private boolean isIgnoreFactTable(AliasedTable table) {
    for (Table ignoreFactTable : ignoreFactTableSet) {
      if (ignoreFactTable.getName().equalsIgnoreCase(table.getTable().getName())) {
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
}
