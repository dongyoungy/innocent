package dyoon.innocent.query;

import com.google.common.collect.HashBasedTable;
import dyoon.innocent.Query;
import dyoon.innocent.Utils;
import dyoon.innocent.data.AliasedTable;
import dyoon.innocent.data.Column;
import dyoon.innocent.data.FactDimensionJoin;
import dyoon.innocent.data.PartitionCandidate;
import dyoon.innocent.data.Predicate;
import dyoon.innocent.data.PredicateColumn;
import dyoon.innocent.data.Table;
import dyoon.innocent.data.UnorderedPair;
import dyoon.innocent.database.Database;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlShuttle;
import org.pmw.tinylog.Logger;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Created by Dong Young Yoon on 2019-02-01. */
public class PartitionTableReplacer extends SqlShuttle {

  private Database database;
  private Query query;
  private Set<Table> allTables;
  private List<Column> allColumns;
  private Set<PartitionCandidate> candidates;
  private Set<Table> factTableSet;
  private Set<Table> ignoreFactTableSet;

  public PartitionTableReplacer(
      Database database,
      Query query,
      Set<Table> allTables,
      List<Column> allColumns,
      Set<PartitionCandidate> candidates,
      Set<Table> factTableSet,
      Set<Table> ignoreFactTableSet) {
    this.database = database;
    this.query = query;
    this.allTables = allTables;
    this.allColumns = allColumns;
    this.candidates = candidates;
    this.factTableSet = factTableSet;
    this.ignoreFactTableSet = ignoreFactTableSet;
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

      Set<AliasedTable> factTables = new HashSet<>();

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
            factTables.add(factTable);
          }
          Set<UnorderedPair<SqlIdentifier>> joinPairSet = joinTable.get(factTable, dimTable);
          joinPairSet.add(keyPair);
        }
      }

      Set<FactDimensionJoin> factDimensionJoinSet = new HashSet<>();
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

      // replace for each fact table
      for (AliasedTable factTable : factTables) {
        Table currentFactTable = factTable.getTable();

        List<PartitionCandidate> candidateList = new ArrayList<>();
        Map<PartitionCandidate, Set<Predicate>> pcToPredMap = new HashMap<>();

        // Rank partition candidates
        for (PartitionCandidate candidate : candidates) {
          candidate.setQueryCost(Long.MAX_VALUE);
          Set<FactDimensionJoin> joinSet = candidate.getPrejoin().getJoinSet();
          Set<Column> currentColumnSet = new HashSet<>();
          Set<Predicate> usedPredicates = new HashSet<>();
          int multiplier = 1;

          for (FactDimensionJoin join : factDimensionJoinSet) {
            // if candidate works for the given join...
            if (join.getFactTable().equals(currentFactTable)) {
              if (joinSet.contains(join)) {
                Set<PredicateColumn> usedColumns = candidate.findUsedColumns(join);
                for (PredicateColumn predicateColumn : usedColumns) {
                  currentColumnSet.add(predicateColumn.getColumn());
                  usedPredicates.add(predicateColumn.getPredicate());
                  multiplier *= predicateColumn.getMultiplier();
                }
              }
            }
          }

          if (!currentColumnSet.isEmpty()) {
            PartitionCandidate pcForThisQuery =
                new PartitionCandidate(candidate.getPrejoin(), currentColumnSet);
            try {
              database.calculateStatsForPartitionCandidate(pcForThisQuery);
            } catch (SQLException e) {
              e.printStackTrace();
            }
            long queryCost = multiplier * pcForThisQuery.getMaxParitionSize();
            candidate.setQueryCost(queryCost);
            candidateList.add(candidate);
            pcToPredMap.put(candidate, usedPredicates);
          }
        }
        candidateList.sort((o1, o2) -> (int) (o1.getQueryCost() - o2.getQueryCost()));

        if (!candidateList.isEmpty()) {
          PartitionCandidate best = candidateList.get(0);
          // replace fact table + where clauses
          SqlNode where = select.getWhere();
          Set<Predicate> predToReplace = pcToPredMap.get(best);

          PartitionPredicateReplacer predReplacer = new PartitionPredicateReplacer(predToReplace);
          select.setWhere(where.accept(predReplacer));

          SqlIdentifier newFactTableId =
              new SqlIdentifier(
                  Arrays.asList(database.getInnocentDatabase(), best.getPartitionTableName()),
                  SqlParserPos.ZERO);

          IdReplacer tableReplacer =
              new IdReplacer(
                  newFactTableId, (SqlIdentifier) factTable.getTable().getCorrespondingNode());
          SqlNode from = select.getFrom();
          select.setFrom(from.accept(tableReplacer));
        }
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
