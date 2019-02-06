package dyoon.innocent;

import com.google.common.base.Joiner;
import com.google.common.base.Stopwatch;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Sets;
import com.google.common.math.Stats;
import dyoon.innocent.data.AliasedTable;
import dyoon.innocent.data.Column;
import dyoon.innocent.data.FactDimensionJoin;
import dyoon.innocent.data.Join;
import dyoon.innocent.data.PartitionCandidate;
import dyoon.innocent.data.PartitionSpace;
import dyoon.innocent.data.Predicate;
import dyoon.innocent.data.PredicateColumn;
import dyoon.innocent.data.Prejoin;
import dyoon.innocent.data.Table;
import dyoon.innocent.data.UnorderedPair;
import dyoon.innocent.database.Database;
import dyoon.innocent.lp.ILPSolver;
import dyoon.innocent.query.AggregationColumnResolver;
import dyoon.innocent.query.AliasReplacer;
import dyoon.innocent.query.ErrorPropagator;
import dyoon.innocent.query.JoinAndPredicateFinder;
import dyoon.innocent.query.PartitionTableReplacer;
import dyoon.innocent.query.QueryTransformer;
import dyoon.innocent.query.QueryVisitor;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlWith;
import org.apache.calcite.sql.dialect.HiveSqlDialect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.checkerframework.checker.nullness.compatqual.NullableDecl;
import org.pmw.tinylog.Logger;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

/** Created by Dong Young Yoon on 10/19/18. */
public class InnocentEngine {

  class MaxPartitionPredicate implements com.google.common.base.Predicate<Set<Column>> {

    private long maxPartitionPerTable;

    public MaxPartitionPredicate(long maxPartitionPerTable) {
      this.maxPartitionPerTable = maxPartitionPerTable;
    }

    @Override
    public boolean apply(@NullableDecl Set<Column> columns) {
      long val = 1;
      for (Column column : columns) {
        val *= column.getNumDistinctValues();
      }
      return !columns.isEmpty() && (val < this.maxPartitionPerTable);
    }
  }

  private Args args;
  private String timeCreated;
  private boolean isSampleUsed = false;
  private Database database;
  private Map<String, Double> origRunTimeCache;

  // for partition analysis
  private static final int MAX_COLUMN_TO_CONSIDER_PER_FACT_TABLE = 15;
  private static final int MAX_COLUMN_PER_PARTITIONED_TABLE = 3;

  private static final double DEFAULT_PREJOIN_UNIFORM_RATIO = 0.01;
  private Set<Table> factTableSet;
  private Set<Table> ignoreFactTableSet;
  private SortedSet<Query> allQueries;
  private Set<Table> allTables;
  private List<Column> allColumns;
  private Set<PartitionCandidate> partitionCandidates;
  private Set<Prejoin> prejoinSet;
  private com.google.common.collect.Table<Table, Column, PartitionSpace> partitionSpaceTable;
  private Map<Column, Integer> predicateColumnFreqTable;

  private Set<FactDimensionJoin> factDimensionJoinSet;

  public InnocentEngine(Database database, Args args, String timestamp) {
    this.database = database;
    this.isSampleUsed = false;
    this.timeCreated = timestamp;
    this.origRunTimeCache = new HashMap<>();
    this.args = args;

    this.allQueries = new TreeSet<>();
    this.allTables = new HashSet<>();
    this.allColumns = new ArrayList<>();
    this.partitionCandidates = new HashSet<>();
    this.partitionSpaceTable = HashBasedTable.create();
    this.prejoinSet = new HashSet<>();
    this.predicateColumnFreqTable = new HashMap<>();

    this.factDimensionJoinSet = new HashSet<>();

    try {
      allTables = database.getAllTableAndColumns(args.getDatabase());
    } catch (SQLException e) {
      e.printStackTrace();
    }
    allColumns = Utils.getAllColumns(allTables);
  }

  public InnocentEngine() {
    this.database = null;
    this.isSampleUsed = false;
    this.timeCreated = "";
    this.origRunTimeCache = new HashMap<>();
    this.prejoinSet = new HashSet<>();
    this.predicateColumnFreqTable = new HashMap<>();
    this.partitionCandidates = new HashSet<>();

    this.allQueries = new TreeSet<>();
    this.allTables = new HashSet<>();
    this.allColumns = new ArrayList<>();
    this.factDimensionJoinSet = new HashSet<>();

    try {
      allTables = database.getAllTableAndColumns(args.getDatabase());
    } catch (SQLException e) {
      e.printStackTrace();
    }
    allColumns = Utils.getAllColumns(allTables);
  }

  public boolean isSampleUsed() {
    return isSampleUsed;
  }

  public SqlNode parse(String sql, QueryVisitor visitor)
      throws ClassNotFoundException, SqlParseException {
    Class.forName("org.apache.calcite.jdbc.Driver");
    SqlParser sqlParser = SqlParser.create(sql);
    SqlNode node = sqlParser.parseQuery();

    return node.accept(visitor);
  }

  public Set<Prejoin> buildPrejoins() throws SQLException {
    String targetDatabase = args.getDatabaseForInnocent();
    String sourceDatabase = args.getDatabase();
    Set<FactDimensionJoin> joinWithPredicateSet = new HashSet<>();
    for (FactDimensionJoin factDimensionJoin : factDimensionJoinSet) {
      if (!factDimensionJoin.getPredicates().isEmpty()) {
        joinWithPredicateSet.add(factDimensionJoin);
      }
    }
    Set<Table> factTablesToConsider = new HashSet<>(this.factTableSet);
    factTablesToConsider.removeAll(this.ignoreFactTableSet);

    Set<Prejoin> prejoinsToBuild = new HashSet<>();
    for (FactDimensionJoin join : joinWithPredicateSet) {
      if (factTablesToConsider.contains(join.getFactTable())) {
        Prejoin prejoinToAddJoin = this.findExisitingPrejoinForJoin(prejoinsToBuild, join);
        if (prejoinToAddJoin != null) {
          prejoinToAddJoin.addJoin(join);
        } else {
          Prejoin newPrejoin = new Prejoin(join.getFactTable());
          newPrejoin.addJoin(join);
          prejoinsToBuild.add(newPrejoin);
        }
      }
    }

    // build prejoin with fact table sample
    for (Prejoin prejoin : prejoinsToBuild) {
      database.constructPrejoinWithUniformSample(
          targetDatabase, sourceDatabase, prejoin, DEFAULT_PREJOIN_UNIFORM_RATIO);
    }

    return prejoinsToBuild;
  }

  private Prejoin findExisitingPrejoinForJoin(Set<Prejoin> prejoins, FactDimensionJoin join) {
    Prejoin prejoinFound = null;

    for (Prejoin prejoin : prejoins) {
      if (prejoin.getFactTable().equals(join.getFactTable())) {
        if (prejoin.getJoinSet().contains(join)) {
          prejoinFound = prejoin;
          break;
        } else if (!prejoin.containDimension(join)) {
          prejoinFound = prejoin;
          break;
        }
      }
    }

    return prejoinFound;
  }

  public Set<PartitionCandidate> createPartitionCandidates(
      Set<Prejoin> prejoins, int maxPartitionsPerTable) throws SQLException {

    Set<PartitionCandidate> candidates = new HashSet<>();

    // build partition candidates from each prejoin
    for (Prejoin prejoin : prejoins) {
      // get all predicate columns
      Set<Column> predColumnSet = new HashSet<>();
      Map<Column, Integer> predColumnFreq = new HashMap<>();
      for (FactDimensionJoin join : prejoin.getJoinSet()) {
        for (Predicate predicate : join.getPredicates()) {
          Column column = predicate.getColumn();
          predColumnSet.add(column);
          if (!predColumnFreq.containsKey(column)) {
            predColumnFreq.put(column, 1);
          } else {
            int freq = predColumnFreq.get(column);
            predColumnFreq.put(column, freq + 1);
          }
        }
      }

      PriorityQueue<Pair<Column, Integer>> predColumnQueue =
          new PriorityQueue<>(
              predColumnSet.size(),
              (o1, o2) ->
                  10000 * (o2.getRight() - o1.getRight())
                      + o1.getLeft().getName().compareTo(o2.getLeft().getName()));

      for (Map.Entry<Column, Integer> entry : predColumnFreq.entrySet()) {
        predColumnQueue.add(ImmutablePair.of(entry.getKey(), entry.getValue()));
      }

      predColumnSet.clear();

      int numCol = 0;
      while (numCol < MAX_COLUMN_TO_CONSIDER_PER_FACT_TABLE && !predColumnQueue.isEmpty()) {
        Pair<Column, Integer> pair = predColumnQueue.poll();
        predColumnSet.add(pair.getLeft());
        ++numCol;
      }

      for (Column column : predColumnSet) {
        this.database.calculateNumDistinct(column);
      }

      //      // get num distinct values for each predicate column
      //      // build a map {table -> column}
      //      Map<String, Set<Column>> tableToColumnSetMap = new HashMap<>();
      //      for (Column column : predColumnSet) {
      //        this.database.calculateNumDistinct(column);
      //        if (!tableToColumnSetMap.containsKey(column.getTable())) {
      //          tableToColumnSetMap.put(column.getTable(), new HashSet<>());
      //        }
      //        tableToColumnSetMap.get(column.getTable()).add(column);
      //      }

      //      Set<Set<Column>> possibleColumnSets = new HashSet<>();
      //      // for columns in each dimension table, calculate possible column sets
      //      for (Set<Column> columnSet : tableToColumnSetMap.values()) {
      //        // get powerset first
      //        Set<Set<Column>> columnPowerSet = Sets.powerSet(columnSet);
      //
      //        // filter ones that have too many partitions
      //        MaxPartitionPredicate maxPartitionPredicate =
      //            new MaxPartitionPredicate(maxPartitionsPerTable);
      //        Set<Set<Column>> filteredSets = Sets.filter(columnPowerSet, maxPartitionPredicate);
      //        possibleColumnSets.addAll(filteredSets);
      //      }
      //
      //      // generate candidates
      //      for (Set<Column> columnSet : possibleColumnSets) {
      //        PartitionCandidate candidate = new PartitionCandidate(prejoin, columnSet);
      //        this.database.calculateStatsForPartitionCandidate(candidate);
      //        candidates.add(candidate);
      //      }

      //      Set<Set<Column>> powerSet = Sets.powerSet(predColumnSet);
      Set<Set<Column>> columnPowerSet = new HashSet<>();
      //      for (Set<Column> columns : powerSet) {
      //        if (columns.size() < 4) {
      //          columnPowerSet.add(columns);
      //        }
      //      }

      for (int i = 1; i <= MAX_COLUMN_PER_PARTITIONED_TABLE && i <= predColumnSet.size(); ++i) {
        columnPowerSet.addAll(Sets.combinations(predColumnSet, i));
      }

      // filter ones that have too many partitions
      MaxPartitionPredicate maxPartitionPredicate =
          new MaxPartitionPredicate(maxPartitionsPerTable);
      Set<Set<Column>> possibleColumnSets = Sets.filter(columnPowerSet, maxPartitionPredicate);

      // generate candidates
      for (Set<Column> columnSet : possibleColumnSets) {
        PartitionCandidate candidate = new PartitionCandidate(prejoin, columnSet);
        this.database.calculateStatsForPartitionCandidate(candidate);
        candidates.add(candidate);
      }
    }

    return candidates;
  }

  public void buildPartitions(Set<PartitionCandidate> candidates) throws SQLException {
    for (PartitionCandidate candidate : candidates) {
      this.database.buildPartitionTable(candidate);
    }
  }

  public void runQueryWithBestPartition(Query q) throws SQLException, SqlParseException {
    Set<PartitionCandidate> availablePartitionedTables =
        this.database.getAvailablePartitionedTables();

    if (this.allTables.isEmpty()) {
      allTables = database.getAllTableAndColumns(args.getDatabase());
      allColumns = Utils.getAllColumns(allTables);
    }

    SqlParser sqlParser = SqlParser.create(q.getQuery());
    SqlNode node = sqlParser.parseQuery();
    String[] factTables = args.getFactTables().split(",");
    String[] ignoreFactTables = args.getIgnoreFactTables().split(",");
    this.factTableSet = Utils.getTableSetWithNames(allTables, factTables);
    this.ignoreFactTableSet = Utils.getTableSetWithNames(allTables, ignoreFactTables);

    allQueries.add(q);

    PartitionTableReplacer replacer =
        new PartitionTableReplacer(
            database,
            q,
            allTables,
            allColumns,
            availablePartitionedTables,
            factTableSet,
            ignoreFactTableSet);

    SqlNode newNode = node.accept(replacer);

    SqlDialect dialect = new HiveSqlDialect(SqlDialect.EMPTY_CONTEXT);
    String sql = newNode.toSqlString(dialect).getSql();
    sql = sql.replaceAll("FETCH NEXT ", "LIMIT ");
    sql = sql.replaceAll("ROWS ONLY", "");

    Stopwatch watch = Stopwatch.createStarted();
    database.executeQuery(sql);
    watch.stop();

    System.out.println(
        String.format(
            "Elapsed = %.3f s", (double) watch.elapsed(TimeUnit.MILLISECONDS) / (double) 1000));
  }

  public Set<PartitionCandidate> findBestPartitionGreedy(
      Set<PartitionCandidate> candidates, double targetIOBound, int size) throws SQLException {
    Set<PartitionCandidate> bestPartitions = new HashSet<>();
    Set<PartitionCandidate> candidatesToConsider = new HashSet<>(candidates);
    SortedSet<Query> queriesToConsider = new TreeSet<>(this.allQueries);

    for (Table factTable : factTableSet) {

      int rank = 1;
      Set<PartitionCandidate> bestPartitionsForCurrentTable = new HashSet<>();
      while (bestPartitionsForCurrentTable.size() < size) {
        Set<PartitionCandidate> currentCandidates = new HashSet<>();

        for (PartitionCandidate candidate : candidatesToConsider) {
          if (!candidate.getPrejoin().getFactTable().equals(factTable)) {
            continue;
          }

          currentCandidates.add(candidate);
          int numQueryWithTargetIOBound = 0;
          double totalIOReduction = 0;
          candidate.clearQueries();
          for (Query query : queriesToConsider) {
            Map<
                    Table,
                    com.google.common.collect.Table<
                        Table, Set<UnorderedPair<Column>>, Set<Predicate>>>
                predicateTableMap = query.getPredicateTableMap();
            Set<PredicateColumn> usedColumnSet = findUsedColumnSet(candidate, predicateTableMap);
            if (!usedColumnSet.isEmpty()) {
              Set<Column> currentColumnSet = new HashSet<>();
              int multiplier = 1;
              for (PredicateColumn predicateColumn : usedColumnSet) {
                currentColumnSet.add(predicateColumn.getColumn());
                multiplier *= predicateColumn.getMultiplier();
              }

              PartitionCandidate pcForThisQuery =
                  new PartitionCandidate(candidate.getPrejoin(), currentColumnSet);
              database.calculateStatsForPartitionCandidate(pcForThisQuery);

              long reducedQueryCost = 0;
              long queryCost = 0;
              for (AliasedTable table : query.getTables()) {
                if (factTableSet.contains(table.getTable())) {
                  long tableSize = this.database.getTableSize(table.getTable());
                  Table fact = table.getTable();
                  query.setCostIfNull(fact, tableSize);
                  query.setReducedCostIfNull(fact, tableSize);

                  queryCost += query.getCost(fact);
                  long currentReducedCost = query.getReducedCost(fact);
                  if (candidate.getPrejoin().getFactTable().equals(table.getTable())) {
                    long reducedCost = (multiplier * pcForThisQuery.getMaxParitionSize());

                    if (reducedCost < currentReducedCost) {
                      candidate.setQueryCost(query, table.getTable(), reducedCost);
                    } else {
                      reducedCost = currentReducedCost;
                    }
                    reducedQueryCost += reducedCost;
                  } else {
                    reducedQueryCost += currentReducedCost;
                  }
                }
              }

              double totalReducedIO = (double) reducedQueryCost / (double) queryCost;
              double currentReducedIO =
                  (double) reducedQueryCost / (double) query.getTotalCurrentReducedCost();
              if (totalReducedIO < targetIOBound) {
                ++numQueryWithTargetIOBound;
                candidate.addQuery(query);
              } else if (currentReducedIO < 1) {
                totalIOReduction += (1 - currentReducedIO);
                candidate.addEffectiveQuery(query);
              }
            }
          }
          candidate.setNumQueryMeetingIOBound(numQueryWithTargetIOBound);
          candidate.setTotalIOReductionForOtherQueries(totalIOReduction);
        }

        if (currentCandidates.isEmpty()) {
          break;
        }

        List<PartitionCandidate> pcList = new ArrayList<>(currentCandidates);
        // sort to find best candidate
        pcList.sort(
            (o1, o2) -> {
              int comp1 =
                  (o2.getNumQueryMeetingIOBound() - o1.getNumQueryMeetingIOBound()) * 100000;
              if (comp1 != 0) {
                return comp1;
              } else {
                return (int)
                    ((o2.getTotalIOReductionForOtherQueries() * 100)
                        - (o1.getTotalIOReductionForOtherQueries() * 100));
              }
            });

        PartitionCandidate best = pcList.get(0);
        if (best.getNumQueryMeetingIOBound() == 0
            && best.getTotalIOReductionForOtherQueries() == 0) {
          break;
        }
        best.setIOBound(targetIOBound);
        best.setRank(rank++);

        bestPartitionsForCurrentTable.add(best);
        candidatesToConsider.remove(best);
        queriesToConsider.removeAll(best.getQueriesMeetingIOBounds());

        for (com.google.common.collect.Table.Cell<Query, Table, Long> cell :
            best.getQueryCostTable().cellSet()) {
          Query q = cell.getRowKey();
          assert q != null;
          q.setReducedCost(cell.getColumnKey(), cell.getValue());
        }
      }
      bestPartitions.addAll(bestPartitionsForCurrentTable);
    }
    return bestPartitions;
  }

  private Set<PredicateColumn> findUsedColumnSet(
      PartitionCandidate candidate,
      Map<Table, com.google.common.collect.Table<Table, Set<UnorderedPair<Column>>, Set<Predicate>>>
          predicateTableMap) {
    Set<PredicateColumn> usedColumns = new HashSet<>();
    for (Table fact : predicateTableMap.keySet()) {
      for (com.google.common.collect.Table.Cell<Table, Set<UnorderedPair<Column>>, Set<Predicate>>
          cell : predicateTableMap.get(fact).cellSet()) {
        Table dim = cell.getRowKey();
        Set<UnorderedPair<Column>> joinColumns = cell.getColumnKey();
        Set<Predicate> predicates = cell.getValue();

        Set<PredicateColumn> columns =
            candidate.findUsedColumns(fact, dim, joinColumns, predicates);
        usedColumns.addAll(columns);
      }
    }
    return usedColumns;
  }

  public void runQueryAnalysis(Query q)
      throws ClassNotFoundException, SqlParseException, SQLException {
    Class.forName("org.apache.calcite.jdbc.Driver");
    Logger.info("Analyzing query {}", q.getId());

    if (this.allTables.isEmpty()) {
      allTables = database.getAllTableAndColumns(args.getDatabase());
      allColumns = Utils.getAllColumns(allTables);
    }

    SqlParser sqlParser = SqlParser.create(q.getQuery());
    SqlNode node = sqlParser.parseQuery();
    String[] factTables = args.getFactTables().split(",");
    String[] ignoreFactTables = args.getIgnoreFactTables().split(",");
    this.factTableSet = Utils.getTableSetWithNames(allTables, factTables);
    this.ignoreFactTableSet = Utils.getTableSetWithNames(allTables, ignoreFactTables);

    allQueries.add(q);

    JoinAndPredicateFinder joinFinder =
        new JoinAndPredicateFinder(q, allTables, allColumns, factTableSet, ignoreFactTableSet);
    node.accept(joinFinder);
    Set<FactDimensionJoin> joinSet = joinFinder.getFactDimensionJoinSet();
    this.mergeFactDimensionJoin(joinSet);

    //    Set<Join> joinSet = joinFinder.getJoinSet();
    //
    //    Map<Set<Table>, Set<Set<SqlIdentifier>>> joinMap = new HashMap<>();

    //    // construct required prejoins
    //    for (Join join : joinSet) {
    //      Set<Set<SqlIdentifier>> joinKeySets = join.getJoinKeys();
    //      for (Set<SqlIdentifier> joinKeySet : joinKeySets) {
    //        Set<Table> joinTables = new HashSet<>();
    //        // joinKeySet should contain two columns
    //        for (SqlIdentifier column : joinKeySet) {
    //          Table table = Utils.findTableContainingColumn(allTables, column);
    //          if (table == null) {
    //            Logger.error("No table found containing {}", column.getSimple());
    //            return;
    //          }
    //          joinTables.add(table);
    //        }
    //
    //        // check join tables contains our target fact tables +
    //        // should not contain tables that we want to ignore
    //        // this logic should change later to take account for individual fact tables
    //        int numFactTable = Utils.containsTableAny(joinTables, factTables);
    //        int numIgnoreFactTable = Utils.containsTableAny(joinTables, ignoreFactTables);
    //
    //        if (numFactTable == 1 && numIgnoreFactTable == 0) {
    //          if (!joinMap.containsKey(joinTables)) {
    //            joinMap.put(joinTables, new HashSet<>());
    //          }
    //          Set<Set<SqlIdentifier>> joinKeys = joinMap.get(joinTables);
    //          joinKeys.add(joinKeySet);
    //        }
    //      }
    //    }
    //
    //    for (Map.Entry<Set<Table>, Set<Set<SqlIdentifier>>> entry : joinMap.entrySet()) {
    //      Set<Set<String>> joinKeyStringSet = convertToString(entry.getValue());
    //      Prejoin newPrejoin = new Prejoin(entry.getKey(), joinKeyStringSet);
    //      newPrejoin.addQuery(q);
    //      prejoinSet.add(newPrejoin);
    //    }
    //
    //    for (Predicate predicate : q.getPredicates()) {
    //      Column column = predicate.getColumn();
    //      if (!predicateColumnFreqTable.containsKey(column)) {
    //        predicateColumnFreqTable.put(column, 1);
    //      } else {
    //        int freq = predicateColumnFreqTable.get(column);
    //        predicateColumnFreqTable.put(column, freq + 1);
    //      }
    //    }
  }

  private void mergeFactDimensionJoin(Set<FactDimensionJoin> joinSet) {
    for (FactDimensionJoin join : joinSet) {
      if (factDimensionJoinSet.contains(join)) {
        for (FactDimensionJoin factDimensionJoin : factDimensionJoinSet) {
          if (factDimensionJoin.equals(join)) {
            factDimensionJoin.addPredicateAll(join.getPredicates());
          }
        }
      } else {
        factDimensionJoinSet.add(join);
      }
    }
  }

  private Set<Set<String>> convertToString(Set<Set<SqlIdentifier>> value) {
    Set<Set<String>> stringSet = new HashSet<>();
    for (Set<SqlIdentifier> set : value) {
      Set<String> newSet = new HashSet<>();
      for (SqlIdentifier id : set) {
        newSet.add(id.names.get(id.names.size() - 1));
      }
      stringSet.add(newSet);
    }
    return stringSet;
  }

  public void runPartitionAnalysis(Query q)
      throws ClassNotFoundException, SqlParseException, SQLException {
    Class.forName("org.apache.calcite.jdbc.Driver");
    SqlParser sqlParser = SqlParser.create(q.getQuery());
    SqlNode node = sqlParser.parseQuery();
    String[] factTables = args.getFactTables().split(",");

    if (this.allTables.isEmpty()) {
      allTables = database.getAllTableAndColumns(args.getDatabase());
      allColumns = Utils.getAllColumns(allTables);
    }

    JoinAndPredicateFinder joinFinder = new JoinAndPredicateFinder(q, allTables, allColumns);
    node.accept(joinFinder);

    Set<Join> joinSet = joinFinder.getJoinSet();

    // do it for each fact table
    for (String factTable : factTables) {
      Table t = Utils.findTableByName(allTables, factTable);
      for (Join join : joinSet) {
        if (join.getTables().contains(t)) {
          for (Predicate p : join.getPredicates()) {
            Column c = p.getColumn();
            if (!partitionSpaceTable.contains(t, c)) {
              partitionSpaceTable.put(t, c, new PartitionSpace(t, c));
            }
            PartitionSpace ps = partitionSpaceTable.get(t, c);
            ps.addBoundary(p);
            ps.addQuery(q);
          }
        }
      }
    }

    Logger.debug("Partition analysis done for query {}", q.getId());
  }

  public void buildPartitions() {
    for (PartitionSpace space : partitionSpaceTable.values()) {
      space.createPartitions();
    }
  }

  public void findBestColumnsForPartition(int k) {
    Map<Table, List<PartitionSpace>> tableToPartitionSpace = new HashMap<>();
    for (Table table : partitionSpaceTable.rowKeySet()) {
      List<PartitionSpace> partitionSpaces = new ArrayList<>();
      for (PartitionSpace ps : partitionSpaceTable.row(table).values()) {
        partitionSpaces.add(ps);
      }
      while (k > 0) {
        List<PartitionSpace> bestPartitionSpaces =
            ILPSolver.solveForBestColumnForPartition(partitionSpaces, k);
        if (!tableToPartitionSpace.containsKey(table)) {
          tableToPartitionSpace.put(table, new ArrayList<>());
        }
        List<PartitionSpace> list = tableToPartitionSpace.get(table);
        list.addAll(bestPartitionSpaces);
        partitionSpaces.removeAll(bestPartitionSpaces);
        k -= bestPartitionSpaces.size();
        // if all queries are covered by less than k columns,
        // repeat to have extra columns for partitions
      }
    }
    Logger.debug("Found best columns for partitioning");
  }

  public AQPInfo rewriteWithSample(Query q, StratifiedSample s)
      throws ClassNotFoundException, SQLException, SqlParseException {
    return this.rewriteWithSample(q, s, true, false);
  }

  public AQPInfo rewriteWithSample(Query q, StratifiedSample s, boolean isWithError)
      throws ClassNotFoundException, SQLException, SqlParseException {
    return this.rewriteWithSample(q, s, isWithError, false);
  }

  public AQPInfo rewriteWithSample(
      Query q, StratifiedSample s, boolean isWithError, boolean doErrorPropagation)
      throws SqlParseException, ClassNotFoundException, SQLException {

    this.isSampleUsed = false;

    Class.forName("org.apache.calcite.jdbc.Driver");
    SqlParser sqlParser = SqlParser.create(q.getQuery());
    SqlNode node = sqlParser.parseQuery();

    if (s != null) {
      List<String> sampleTableColumns =
          (database != null)
              ? database.getColumns(s.getTable().getName())
              : Arrays.asList("c1", "c2", "c3", "c4"); // latter is for testing
      QueryTransformer transformer =
          new QueryTransformer(s, args.getDatabaseForInnocent(), sampleTableColumns, isWithError);
      SqlNode newNode = node.accept(transformer);

      newNode = removeOrderBy(newNode);

      if (transformer.approxCount() > 0) {
        this.isSampleUsed = true;
        List<Pair<SqlIdentifier, SqlIdentifier>> aggAliasPairList =
            transformer.getAggAliasPairList();
        List<SqlIdentifier> aggAliasList = new ArrayList<>();
        for (Pair<SqlIdentifier, SqlIdentifier> pair : aggAliasPairList) {
          aggAliasList.add(pair.getLeft());
        }

        AggregationColumnResolver resolver = new AggregationColumnResolver(aggAliasList);
        newNode.accept(resolver);

        if (doErrorPropagation) {
          ErrorPropagator errorPropagator =
              new ErrorPropagator(aggAliasPairList, transformer.getTransformedSelectListSet());
          newNode = newNode.accept(errorPropagator);
        }

        SqlDialect dialect = new HiveSqlDialect(SqlDialect.EMPTY_CONTEXT);
        q.setAqpQuery(newNode.toSqlString(dialect).toString());

        AQPInfo aqpInfo =
            new AQPInfo(q, s, resolver.getExpressionList(), aggAliasPairList, newNode);
        aqpInfo.addErrorQueries(transformer.getSelectForError());

        return aqpInfo;
      }
    }

    return null;
  }

  private SqlNode removeOrderBy(SqlNode node) {
    if (node instanceof SqlOrderBy) {
      SqlOrderBy orderBy = (SqlOrderBy) node;
      return orderBy.query;
    }
    return node;
  }

  public void runAQPQueryAndCompare(Query q, AQPInfo aqpInfo, Args args) throws SQLException {
    if (q.getAqpQuery().isEmpty()) {
      Logger.error("AQP query is empty for {}. Query will not be run.", q.getId());
      return;
    }

    if (args.isMeasureTime() && args.getClearCacheScript().isEmpty()) {
      Logger.error("You must provide a script for clearing cache to measure time.");
      return;
    }

    double origTime = 0;
    double sampleTime = 0;

    try {
      // run orig query
      if (origRunTimeCache.containsKey(q.getId())
          && database.checkTableExists(q.getResultTableName())) {
        origTime = origRunTimeCache.get(q.getId());
      } else {
        origTime = database.runQueryAndSaveResult(q, args);
        origRunTimeCache.put(q.getId(), origTime);
      }
      // run AQP query
      sampleTime = database.runQueryWithSampleAndSaveResult(aqpInfo, args);
    } catch (Exception e) {
      Logger.error(e);
      return;
    }

    String resultDatabase = args.getDatabase() + Database.RESULT_DATABASE_SUFFIX;
    String originalResultTableName = q.getResultTableName();
    String aqpResultTableName = aqpInfo.getAQPResultTableName();

    List<String> originalResultColumnNames =
        database.getColumns(resultDatabase, originalResultTableName);
    List<String> aqpResultColumnNames = database.getColumns(resultDatabase, aqpResultTableName);
    List<ColumnType> aqpResultColumnTypes = aqpInfo.getColumnTypeList();

    if (aqpResultColumnNames.size() != aqpResultColumnTypes.size()) {
      // if somehow this check fails, try again with result table columns
      aqpResultColumnTypes.clear();
      List<String> aggColumns = new ArrayList<>();
      for (int i = 0; i < aqpResultColumnNames.size(); ++i) {
        String colName = aqpResultColumnNames.get(i);
        if (aqpInfo.isColumnInExpression(colName)) {
          aqpResultColumnTypes.add(ColumnType.AGG);
          aggColumns.add(colName);
        } else if (colName.contains("_rel_error")) {
          aqpResultColumnTypes.add(ColumnType.REL_ERROR);
        } else if (colName.contains("_error")) {
          aqpResultColumnTypes.add(ColumnType.ERROR);
        } else {
          aqpResultColumnTypes.add(ColumnType.NON_AGG);
        }
      }
      for (String column : aggColumns) {
        boolean sanityCheck = false;
        for (String s : aqpResultColumnNames) {
          if (s.matches(String.format("%s_\\d+_rel_error", column))) {
            sanityCheck = true;
          }
        }
        if (!sanityCheck) {
          Logger.error(
              "# of column names and types do not match: {} != {}",
              aqpResultColumnNames.size(),
              aqpResultColumnTypes.size());
          return;
        }
      }

      //      if (!sanityCheck) {
      //        Logger.error(
      //            "# of column names and types do not match: {} != {}",
      //            aqpResultColumnNames.size(),
      //            aqpResultColumnTypes.size());
      //        return;
      //      }
    }

    // get column names and types
    List<String> nonAggOrigColumns = new ArrayList<>();
    List<String> aggOrigColumns = new ArrayList<>();
    List<String> nonAggAQPColumns = new ArrayList<>();
    List<String> aggAQPColumns = new ArrayList<>();
    List<String> errorColumns = new ArrayList<>();
    List<String> relErrorColumns = new ArrayList<>();

    for (int i = 0; i < aqpResultColumnNames.size(); ++i) {
      String name = aqpResultColumnNames.get(i);
      ColumnType type = aqpResultColumnTypes.get(i);

      switch (type) {
        case NON_AGG:
          String origName =
              originalResultColumnNames.contains(name) ? name : originalResultColumnNames.get(i);
          nonAggAQPColumns.add(name);
          nonAggOrigColumns.add(origName);
          break;
        case AGG:
          origName =
              originalResultColumnNames.contains(name) ? name : originalResultColumnNames.get(i);
          aggAQPColumns.add(name);
          aggOrigColumns.add(origName);
          break;
        case ERROR:
          errorColumns.add(name);
          break;
        case REL_ERROR:
          relErrorColumns.add(name);
          break;
        default:
          Logger.error("Unknown column type: {}", type);
          return;
      }
    }

    List<String> selectItems = new ArrayList<>();
    List<String> evalItems = new ArrayList<>();
    List<String> relErrors = new ArrayList<>();
    for (int i = 0; i < aggAQPColumns.size(); ++i) {
      String origCol = aggOrigColumns.get(i);
      String aqpCol = aggAQPColumns.get(i);
      //      String errCol = errorColumns.get(i);
      evalItems.add(
          String.format(
              "abs(quotient((s.%s - o.%s) * 100000, o.%s) / 100000)", aqpCol, origCol, origCol));
      //      relErrors.add(String.format("avg(s.%s / s.%s) as %s_rel_error", errCol, aqpCol,
      // aqpCol));
    }

    for (String col : relErrorColumns) {
      relErrors.add(String.format("avg(s.%s) as %s", col, col));
    }

    String sumEval = Joiner.on(" + ").join(evalItems);
    String avgPerError =
        String.format("(avg(%s) / %d) as avg_per_error", sumEval, evalItems.size());

    selectItems.add(avgPerError);
    selectItems.addAll(relErrors);

    String selectClause = Joiner.on(",").join(selectItems);
    String fromClause =
        String.format(
            "%s.%s as o, %s.%s as s",
            resultDatabase, originalResultTableName, resultDatabase, aqpResultTableName);

    List<String> joinItems = new ArrayList<>();
    for (int i = 0; i < nonAggOrigColumns.size(); ++i) {
      String origCol = nonAggOrigColumns.get(i);
      String aqpCol = nonAggAQPColumns.get(i);
      joinItems.add(String.format("o.%s = s.%s", origCol, aqpCol));
    }
    String joinClause = Joiner.on(" AND ").join(joinItems);

    String evalSql = String.format("SELECT %s FROM %s", selectClause, fromClause);
    if (!joinClause.isEmpty()) {
      evalSql += String.format(" WHERE %s", joinClause);
    }
    String origGroupCountSql =
        String.format(
            "SELECT count(*) as groupcount from %s.%s", resultDatabase, originalResultTableName);
    String aqpGroupCountSql =
        String.format(
            "SELECT count(*) as groupcount from %s.%s s", resultDatabase, aqpResultTableName);
    if (!joinClause.isEmpty()) {
      aqpGroupCountSql += String.format(", %s o WHERE %s", originalResultTableName, joinClause);
    }

    long origGroupCount = 0, aqpGroupCount = 0;
    double[] errors = new double[selectItems.size()];

    ResultSet rs = database.executeQuery(origGroupCountSql);
    if (rs.next()) {
      origGroupCount = rs.getLong("groupcount");
    }
    rs.close();

    rs = database.executeQuery(aqpGroupCountSql);
    if (rs.next()) {
      aqpGroupCount = rs.getLong("groupcount");
    }
    rs.close();

    rs = database.executeQuery(evalSql);
    if (rs.next()) {
      for (int i = 0; i < errors.length; ++i) {
        errors[i] = rs.getDouble(i + 1);
      }
    }

    List<String> relErrorString = new ArrayList<>();
    for (int i = 0; i < relErrorColumns.size(); ++i) {
      relErrorString.add(
          String.format("%s = %.4f %%", relErrorColumns.get(i), errors[i + 1] * 100));
    }

    double avgRelError = Stats.meanOf(errors);
    double avgPercentError = errors[0];
    double missingGroupRatio = (double) (origGroupCount - aqpGroupCount) / (double) origGroupCount;

    File resultDir = new File(String.format("./results/%s/", this.timeCreated));
    resultDir.mkdirs();

    File resultFile =
        new File(
            String.format(
                "./results/%s/%s", this.timeCreated, aqpInfo.getSample().getSampleTableName()));
    FileWriter fw = null;
    PrintWriter pw = null;
    try {
      fw = new FileWriter(resultFile, true);
      pw = new PrintWriter(fw);
    } catch (IOException e) {
      e.printStackTrace();
    }

    if (pw != null) {
      pw.println(
          String.format(
              "%s,%s,%.4f,%.4f,%.4f,%.4f,%.4f",
              q.getId(),
              aqpInfo.getSample().getSampleTableName(),
              missingGroupRatio,
              avgPercentError,
              avgRelError,
              origTime / 1000,
              sampleTime / 1000));
      pw.flush();
    }

    Logger.info(
        String.format(
            "query '%s' with sample %s gives:\n\tmissing group ratio = %.4f %% (%d out of %d), "
                + "avg. percent error = %.4f %%, avg. rel error = %.4f %%, "
                + "orig time = %.4f s, sample time = %.4f s."
                + "\n\t%s",
            q.getId(),
            aqpInfo.getSample().getSampleTableName(),
            missingGroupRatio * 100,
            aqpGroupCount,
            origGroupCount,
            avgPercentError * 100,
            avgRelError * 100,
            origTime / 1000,
            sampleTime / 1000,
            Joiner.on(", ").join(relErrorString)));
  }

  private SqlSelect getOuterMostSelect(SqlNode node) {
    if (node instanceof SqlOrderBy) {
      // gets rid of LIMIT
      SqlOrderBy orderBy = (SqlOrderBy) node;
      node =
          new SqlOrderBy(
              node.getParserPosition(),
              orderBy.query.clone(SqlParserPos.ZERO),
              orderBy.orderList,
              orderBy.offset,
              null);

      return this.getOuterMostSelect(orderBy.query);
    } else if (node instanceof SqlSelect) {
      return this.cloneSqlSelect((SqlSelect) node);
    } else if (node instanceof SqlWith) {
      SqlWith with = (SqlWith) node;
      return this.cloneSqlSelect((SqlSelect) with.body);
    }
    return null;
  }

  private void replaceSelectListForSampleAlias(SqlSelect modifiedSelect, QueryVisitor visitor) {
    AliasReplacer replacer = new AliasReplacer(visitor.getAliasMap());
    modifiedSelect.accept(replacer);
  }

  private void replaceListForOuterQuery(SqlNodeList selectList) {
    AliasReplacer replacer = new AliasReplacer(true, null);
    selectList.accept(replacer);
  }

  private SqlNodeList getGroupBy(SqlNode node) {
    if (node instanceof SqlWith) {
      SqlWith with = (SqlWith) node;
      SqlSelect select = (SqlSelect) with.body;
      return select.getGroup();
    } else if (node instanceof SqlOrderBy) {
      SqlOrderBy orderBy = (SqlOrderBy) node;
      return this.getGroupBy(orderBy.query);
      //      SqlSelect select = (SqlSelect) orderBy.query;
      //      return select.getGroup();
    } else if (node instanceof SqlSelect) {
      SqlSelect select = (SqlSelect) node;
      return select.getGroup();
    }
    return null;
  }

  private boolean IsInGroupBy(SqlNode node, SqlNodeList groupBy) {
    if (node instanceof SqlIdentifier) {
      SqlIdentifier id = (SqlIdentifier) node;
      for (SqlNode groupByNode : groupBy.getList()) {
        if (groupByNode instanceof SqlIdentifier) {
          SqlIdentifier groupById = (SqlIdentifier) groupByNode;
          if (groupById.equals(id)) {
            return true;
          }
        }
      }
    }
    return false;
  }

  private SqlNodeList cloneSqlNodeList(SqlNodeList list) {
    SqlNodeList newList = new SqlNodeList(SqlParserPos.ZERO);
    for (SqlNode node : list) {
      newList.add((node != null) ? node.clone(SqlParserPos.ZERO) : null);
    }
    return newList;
  }

  private SqlSelect cloneSqlSelect(SqlSelect select) {
    SqlParserPos pos = SqlParserPos.ZERO;
    SqlNodeList selectList =
        (select.getSelectList() != null) ? this.cloneSqlNodeList(select.getSelectList()) : null;
    SqlNode from = (select.getFrom() != null) ? select.getFrom().clone(pos) : null;
    SqlNode where = (select.getWhere() != null) ? select.getWhere().clone(pos) : null;
    SqlNodeList groupBy =
        (select.getGroup() != null) ? this.cloneSqlNodeList(select.getGroup()) : null;
    SqlNode having = (select.getHaving() != null) ? select.getHaving().clone(pos) : null;
    SqlNodeList windowList =
        (select.getWindowList() != null) ? this.cloneSqlNodeList(select.getWindowList()) : null;
    SqlNodeList orderBy =
        (select.getOrderList() != null) ? this.cloneSqlNodeList(select.getOrderList()) : null;
    SqlNode offset = (select.getOffset() != null) ? select.getOffset().clone(pos) : null;
    SqlNode fetch = (select.getFetch() != null) ? select.getFetch().clone(pos) : null;

    return new SqlSelect(
        pos, null, selectList, from, where, groupBy, having, windowList, orderBy, offset, fetch);
  }
}
