package dyoon.innocent.database;

import com.google.common.base.Joiner;
import com.google.common.base.Stopwatch;
import dyoon.innocent.AQPInfo;
import dyoon.innocent.Args;
import dyoon.innocent.InnocentMeta;
import dyoon.innocent.Query;
import dyoon.innocent.StratifiedSample;
import dyoon.innocent.UniformSample;
import dyoon.innocent.data.Column;
import dyoon.innocent.data.FactDimensionJoin;
import dyoon.innocent.data.PartitionCandidate;
import dyoon.innocent.data.Prejoin;
import dyoon.innocent.data.Table;
import dyoon.innocent.data.UnorderedPair;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWith;
import org.apache.calcite.sql.dialect.HiveSqlDialect;
import org.pmw.tinylog.Logger;

import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/** Created by Dong Young Yoon on 10/23/18. */
public class ImpalaDatabase extends Database implements DatabaseImpl {

  private Map<String, Object> cache = new HashMap<>();
  private static final String TIMESTAMP_FORMAT = "yyyy-MM-dd HH:mm:ss.SSS";

  public ImpalaDatabase(
      String host, String sourceDatabase, String innocentDatabase, String user, String password) {
    try {
      Class.forName("com.cloudera.impala.jdbc41.Driver");
      String connString = String.format("jdbc:impala://%s/%s", host, sourceDatabase);
      this.conn = DriverManager.getConnection(connString, user, password);
      this.sourceDatabase = sourceDatabase;
      this.innocentDatabase = innocentDatabase;
      this.cache = new HashMap<>();
      this.meta = InnocentMeta.getInstance(innocentDatabase, this);
    } catch (ClassNotFoundException | SQLException e) {
      e.printStackTrace();
    }
  }

  @Override
  public List<String> getTables() throws SQLException {
    final List<String> tables = new ArrayList<>();
    final ResultSet rs = this.executeQuery(String.format("SHOW TABLES"));
    while (rs.next()) {
      tables.add(rs.getString(1));
    }
    return tables;
  }

  @Override
  public List<String> getTables(String database) throws SQLException {
    final List<String> tables = new ArrayList<>();
    final ResultSet rs = this.executeQuery(String.format("SHOW TABLES IN %s", database));
    while (rs.next()) {
      tables.add(rs.getString(1));
    }
    return tables;
  }

  @Override
  public long getTableSize(Table table) throws SQLException {
    String key = String.format("table size of %s.%s", this.sourceDatabase, table.getName());
    if (cache.containsKey(key)) {
      return (Long) cache.get(key);
    }

    String sql = String.format("SELECT COUNT(*) FROM %s.%s", this.sourceDatabase, table.getName());
    ResultSet rs = this.executeQuery(sql);
    long val = -1;
    if (rs.next()) {
      val = rs.getLong(1);
      cache.put(key, val);
    }
    return val;
  }

  @Override
  public List<String> getColumns(String table) throws SQLException {
    final List<String> columns = new ArrayList<>();
    final ResultSet rs = this.executeQuery(String.format("DESCRIBE %s", table));
    while (rs.next()) {
      columns.add(rs.getString(1));
    }
    return columns;
  }

  @Override
  public List<String> getColumns(String database, String table) throws SQLException {
    final List<String> columns = new ArrayList<>();
    final ResultSet rs = this.executeQuery(String.format("DESCRIBE %s.%s", database, table));
    while (rs.next()) {
      columns.add(rs.getString(1));
    }
    return columns;
  }

  @Override
  public String getColumnType(String table, String column) throws SQLException {
    final ResultSet rs = this.executeQuery(String.format("DESCRIBE %s", table));
    while (rs.next()) {
      if (rs.getString(1).equalsIgnoreCase(column)) {
        return rs.getString(2);
      }
    }
    return null;
  }

  @Override
  public void buildPartitionTable(PartitionCandidate candidate, boolean overwrite) throws SQLException {
    Prejoin prejoin = candidate.getPrejoin();
    Set<Column> columnSet = candidate.getColumnSet();
    Set<FactDimensionJoin> joinSet = prejoin.getJoinSet();
    Table factTable = prejoin.getFactTable();

    SortedSet<String> partitionColumnNames = new TreeSet<>();
    Set<String> partitionColumnTypes = new HashSet<>();
    Set<String> dimTables = new HashSet<>();
    Column maxPartitionColumn = null;
    long maxPartition = -1;
    for (Column column : columnSet) {
      partitionColumnNames.add(column.getName());
      partitionColumnTypes.add(column.getType());
      dimTables.add(column.getTable());
      if (maxPartition < column.getNumDistinctValues()) {
        maxPartitionColumn = column;
        maxPartition = column.getNumDistinctValues();
      }
    }

    Set<UnorderedPair<Column>> joinColumnPairs = new HashSet<>();
    Set<UnorderedPair<Column>> maxPartitionColumnPairs = new HashSet<>();

    for (FactDimensionJoin join : joinSet) {
      if (dimTables.contains(join.getDimensionTable().getName())) {
        joinColumnPairs.addAll(join.getJoinPairs());
        if (maxPartitionColumn.getTable().equals(join.getDimensionTable().getName())) {
          maxPartitionColumnPairs.addAll(join.getJoinPairs());
        }
      }
    }

    String partitionedTableName = candidate.getPartitionTableName();

    if (!overwrite && checkTableExists(this.innocentDatabase, partitionedTableName)) {
      Logger.info(
          "Partitioned table '{}.{}' already exists", this.innocentDatabase, partitionedTableName);

      // update metadata nonetheless
      this.meta.put(partitionedTableName, PartitionCandidate.getJsonString(candidate));
      return;
    }

    if (overwrite) {
      String dropSql =
          String.format(
              "DROP TABLE IF EXISTS %s.%s",
              this.innocentDatabase,
              partitionedTableName);
      this.execute(dropSql);
    }

    Set<Column> factTableColumns = factTable.getColumns();
    List<String> columnStrList = new ArrayList<>();
    List<String> partitionColumnStrList = new ArrayList<>();
    List<String> partitionColumnStrAsList = new ArrayList<>();
    for (Column factTableColumn : factTableColumns) {
      columnStrList.add(factTableColumn.getName() + " " + factTableColumn.getType());
    }
    for (Column partitionColumn : columnSet) {
      // add '_part' as suffix for partition columns
      partitionColumnStrList.add(partitionColumn.getName() + "_part " + partitionColumn.getType());
    }

    String createTableSql =
        String.format(
            "CREATE TABLE IF NOT EXISTS %s.%s (%s) " + "PARTITIONED BY (%s) STORED AS PARQUET",
            this.innocentDatabase,
            partitionedTableName,
            Joiner.on(",").join(columnStrList),
            Joiner.on(",").join(partitionColumnStrList));

    columnStrList.clear();
    partitionColumnStrList.clear();
    for (Column factTableColumn : factTableColumns) {
      columnStrList.add(factTableColumn.getName());
    }
    for (Column partitionColumn : columnSet) {
      partitionColumnStrList.add(partitionColumn.getName() + "_part");
      partitionColumnStrAsList.add(
          partitionColumn.getName() + " AS " + partitionColumn.getName() + "_part");
    }
    List<String> whereClauses = new ArrayList<>();
    for (UnorderedPair<Column> joinColumnPair : joinColumnPairs) {
      whereClauses.add(
          joinColumnPair.getLeft().getName() + " = " + joinColumnPair.getRight().getName());
    }

    List<String> maxPartitionwhereClauses = new ArrayList<>();
    for (UnorderedPair<Column> joinColumnPair : maxPartitionColumnPairs) {
      maxPartitionwhereClauses.add(
          joinColumnPair.getLeft().getName() + " = " + joinColumnPair.getRight().getName());
    }

    this.execute(createTableSql);

    String maxPartitionSql =
        String.format(
            "SELECT DISTINCT %s FROM %s,%s WHERE %s",
            maxPartitionColumn.getName(),
            factTable.getName(),
            maxPartitionColumn.getTable(),
            Joiner.on(" AND ").join(maxPartitionwhereClauses));
    ResultSet rs = this.executeQuery(maxPartitionSql);

    boolean isFirst = true;
    while (rs.next()) {
      String insertSql =
          String.format(
              "INSERT %s TABLE %s.%s PARTITION (%s) SELECT %s,%s FROM %s,%s WHERE %s",
              (isFirst ? "OVERWRITE" : "INTO"),
              this.innocentDatabase,
              partitionedTableName,
              Joiner.on(",").join(partitionColumnStrList),
              Joiner.on(",").join(columnStrList),
              Joiner.on(",").join(partitionColumnStrAsList),
              factTable.getName(),
              Joiner.on(",").join(dimTables),
              Joiner.on(" AND ").join(whereClauses));

      String extraWhere;

      Object obj = rs.getObject(1);
      if (rs.wasNull()) {
        extraWhere = String.format("%s IS NULL", maxPartitionColumn.getName());
      } else if (maxPartitionColumn.getType().equalsIgnoreCase("string")) {
        extraWhere = String.format("%s = '%s'", maxPartitionColumn.getName(), obj.toString());
      } else {
        extraWhere = String.format("%s = %s", maxPartitionColumn.getName(), obj.toString());
      }
      insertSql += " AND " + extraWhere;

      this.execute(insertSql);
      isFirst = false;
    }

    this.execute(String.format("COMPUTE STATS %s.%s", this.innocentDatabase, partitionedTableName));

    // update metadata at the end
    this.meta.put(partitionedTableName, PartitionCandidate.getJsonString(candidate));
  }

  @Override
  public void createStratifiedSample(String database, StratifiedSample s) throws SQLException {
    this.createStratifiedSample(database, database, s);
  }

  @Override
  public void createStratifiedSample(
      String targetDatabase, String sourceDatabase, StratifiedSample s) throws SQLException {
    String sampleTable = s.getSampleTableName();
    String sampleStatTable = s.getSampleTableName() + "___stat";
    String sourceTable = s.getTable().getName();

    long maxGroupSize = this.getMaxGroupSize(s.getTable().getName(), s.getColumnSet());
    if (maxGroupSize != -1 && maxGroupSize < s.getMinRows()) {
      Logger.warn(
          "Sample '{}' will not be created as its max group size is {} when specified size is {}",
          s.getSampleTableName(),
          maxGroupSize,
          s.getMinRows());
      return;
    }

    if (this.checkTableExists(sampleTable) && this.checkTableExists(sampleStatTable)) {
      Logger.info("Sample table '{}' already exists.", sampleTable);
      return;
    }

    this.createDatabaseIfNotExists(targetDatabase);

    final List<String> factTableColumns = this.getColumns(sourceTable);
    final SortedSet<String> sampleColumns = s.getColumnSet();
    final List<String> sampleColumnsWithFactPrefix = new ArrayList<>();
    for (final String column : sampleColumns) {
      sampleColumnsWithFactPrefix.add(String.format("fact.%s", column));
    }

    final String sampleQCSClause = Joiner.on(",").join(sampleColumnsWithFactPrefix);

    final String createSql =
        String.format(
            "CREATE TABLE IF NOT EXISTS %s.%s LIKE %s.%s STORED as parquet",
            targetDatabase, sampleTable, sourceDatabase, sourceTable);

    this.execute(createSql);

    final String insertSql =
        String.format(
            "INSERT OVERWRITE TABLE %s.%s SELECT %s FROM "
                + "(SELECT fact.*, row_number() OVER (PARTITION BY %s ORDER BY %s) as rownum, "
                + "count(*) OVER (PARTITION BY %s ORDER BY %s) as groupsize, "
                + "%d as target_group_sample_size "
                + "FROM %s.%s as fact "
                + "ORDER BY rand()) tmp "
                + "WHERE tmp.rownum <= %d",
            targetDatabase,
            sampleTable,
            Joiner.on(",").join(factTableColumns),
            sampleQCSClause,
            sampleQCSClause,
            sampleQCSClause,
            sampleQCSClause,
            s.getMinRows(),
            sourceDatabase,
            sourceTable,
            s.getMinRows());
    System.err.println(String.format("Executing: %s", insertSql));

    this.execute(insertSql);

    List<String> selectColumns = new ArrayList<>();
    List<String> statTableColumns = new ArrayList<>();
    List<String> joinColumns = new ArrayList<>();
    for (String column : sampleColumns) {
      String type = this.getColumnType(sourceTable, column);
      statTableColumns.add(String.format("%s %s", column, type));
      joinColumns.add(String.format("fact.%s = sample.%s", column, column));
      selectColumns.add(String.format("fact.%s as %s", column, column));
    }

    final String createStatSql =
        String.format(
            "CREATE TABLE IF NOT EXISTS %s.%s "
                + "(%s, groupsize bigint, actualsize bigint) STORED AS parquet",
            targetDatabase, sampleStatTable, Joiner.on(",").join(statTableColumns));
    System.err.println(String.format("Executing: %s", createStatSql));
    this.execute(createStatSql);

    String selectClause = Joiner.on(",").join(selectColumns);
    String groupByClause = Joiner.on(",").join(sampleColumns);
    String joinClause = Joiner.on(" AND ").join(joinColumns);
    final String insertStatSql =
        String.format(
            "INSERT OVERWRITE TABLE %s.%s SELECT %s, sample.groupsize as samplesize, "
                + "fact.groupsize as actualsize FROM "
                + "(SELECT %s, count(*) as groupsize FROM %s.%s GROUP BY %s) sample, "
                + "(SELECT %s, count(*) as groupsize FROM %s.%s GROUP BY %s) fact "
                + "WHERE %s",
            targetDatabase,
            sampleStatTable,
            selectClause,
            groupByClause,
            targetDatabase,
            sampleTable,
            groupByClause,
            groupByClause,
            sourceDatabase,
            sourceTable,
            groupByClause,
            joinClause);
    System.err.println(String.format("Executing: %s", insertStatSql));
    this.execute(insertStatSql);

    this.execute(String.format("COMPUTE STATS %s.%s", targetDatabase, sampleTable));
    this.execute(String.format("COMPUTE STATS %s.%s", targetDatabase, sampleStatTable));
  }

  @Override
  public long getMaxGroupSize(String table, Set<String> groupBys) throws SQLException {
    String key = table + "-" + Joiner.on(",").join(groupBys) + "-maxgroupsize";
    if (cache.containsKey(key)) {
      return (Long) cache.get(key);
    }
    List<String> notNullCond = new ArrayList<>();
    for (String groupBy : groupBys) {
      notNullCond.add(String.format("%s is not null", groupBy));
    }

    String sql =
        String.format(
            "SELECT MAX(groupsize) FROM "
                + "(SELECT count(*) as groupsize FROM %s WHERE %s GROUP BY %s) tmp",
            table, Joiner.on(" AND ").join(notNullCond), Joiner.on(",").join(groupBys));
    ResultSet rs = this.executeQuery(sql);
    if (rs.next()) {
      long val = rs.getLong(1);
      cache.put(key, val);
      return val;
    }
    return -1;
  }

  @Override
  public long calculateNumDistinct(Column column) throws SQLException {

    String key =
        String.format(
            "count distinct of {%s} on %s.%s", column.getName(), sourceDatabase, column.getTable());
    String val = meta.get(key);
    if (val != null) {
      long value = Long.parseLong(val);
      column.setNumDistinctValues(value);
      return value;
    }

    String sql =
        String.format(
            "SELECT COUNT(DISTINCT %s) FROM %s.%s",
            column.getName(), sourceDatabase, column.getTable());

    ResultSet rs = this.executeQuery(sql);
    if (rs.next()) {
      long value = rs.getLong(1);
      column.setNumDistinctValues(value);
      meta.put(key, String.valueOf(value));
    }
    return column.getNumDistinctValues();
  }

  @Override
  public void calculateStatsForPartitionCandidate(PartitionCandidate candidate)
      throws SQLException {
    Prejoin prejoin = candidate.getPrejoin();
    Set<Column> columnSet = candidate.getColumnSet();

    String prejoinTableName = prejoin.getPrejoinTableName();
    double sampleRatio = prejoin.getSampleRatio();

    SortedSet<String> groupByColumns = new TreeSet<>();
    for (Column column : columnSet) {
      groupByColumns.add(column.getName());
    }

    String groupByClause = Joiner.on(",").join(groupByColumns);
    String key =
        String.format(
            "stats_for columns: {%s} on %s.%s", groupByClause, innocentDatabase, prejoinTableName);
    String val = meta.get(key);
    if (val != null) {
      String[] values = val.split(",");
      candidate.setMinPartitionSize(Long.parseLong(values[0]));
      candidate.setMaxPartitionSize(Long.parseLong(values[1]));
      candidate.setAvgPartitionSize(Long.parseLong(values[2]));
      return;
    }

    String sql =
        String.format(
            "SELECT min(groupsize), max(groupsize), avg(groupsize) FROM "
                + "(SELECT COUNT(*) as groupsize FROM %s.%s GROUP BY %s) tmp",
            innocentDatabase, prejoinTableName, groupByClause);

    ResultSet rs = this.executeQuery(sql);
    if (rs.next()) {
      long minGroupSize = (long) (rs.getDouble(1) / sampleRatio);
      long maxGroupSize = (long) (rs.getDouble(2) / sampleRatio);
      long avgGroupSize = (long) (rs.getDouble(3) / sampleRatio);
      candidate.setMinPartitionSize(minGroupSize);
      candidate.setMaxPartitionSize(maxGroupSize);
      candidate.setAvgPartitionSize(avgGroupSize);
      meta.put(key, minGroupSize + "," + maxGroupSize + "," + avgGroupSize);
    }
  }

  @Override
  public Set<PartitionCandidate> getAvailablePartitionedTables() throws SQLException {
    List<String> tables = this.getTables(this.innocentDatabase);
    List<String> partitionedTables = new ArrayList<>();
    for (String table : tables) {
      String[] s = table.split("___");
      if (s.length > 1 && s[1].equalsIgnoreCase("part")) {
        partitionedTables.add(table);
      }
    }

    Set<PartitionCandidate> partitionedTableSet = new HashSet<>();
    for (String p : partitionedTables) {
      String s = this.meta.get(p);
      if (s != null) {
        partitionedTableSet.add(PartitionCandidate.createFromJsonString(s));
      }
    }

    return partitionedTableSet;
  }

  @Override
  public double runQueryAndSaveResult(Query q, Args args) throws SQLException {
    String resultTable = q.getResultTableName();
    String resultDatabase = args.getDatabase() + Database.RESULT_DATABASE_SUFFIX;

    this.createDatabaseIfNotExists(resultDatabase);

    if (this.checkTableExists(resultDatabase, resultTable)
        && !args.isOverwrite()
        && !args.isMeasureTime()) {
      return 0;
    }

    if (args.isOverwrite() || args.isMeasureTime()) {
      this.execute(String.format("DROP TABLE IF EXISTS %s.%s", resultDatabase, resultTable));
    }

    String sql =
        String.format(
            "CREATE TABLE %s.%s STORED AS parquet AS %s",
            resultDatabase, resultTable, q.getQuery());
    sql = sql.replaceAll("ASYMMETRIC", "");
    sql = sql.replaceAll("LIMIT 100", "");
    sql = sql.replaceAll("limit 100", "");
    sql = sql.replaceAll("ROWS ONLY", "");
    sql = sql.replaceAll("FETCH NEXT", "LIMIT");

    if (args.isMeasureTime()) {
      this.clearCache(args.getClearCacheScript());
    }
    Stopwatch watch = Stopwatch.createStarted();
    this.execute(sql);
    watch.stop();
    return watch.elapsed(TimeUnit.MILLISECONDS);
  }

  @Override
  public double runQueryWithSampleAndSaveResult(AQPInfo info, Args args) throws SQLException {
    String resultTable = info.getAQPResultTableName();
    String resultDatabase = args.getDatabase() + Database.RESULT_DATABASE_SUFFIX;

    this.createDatabaseIfNotExists(resultDatabase);

    if (this.checkTableExists(resultDatabase, resultTable)
        && !args.isOverwrite()
        && !args.isMeasureTime()) {
      return 0;
    }

    if (args.isOverwrite() || args.isMeasureTime()) {
      this.execute(String.format("DROP TABLE IF EXISTS %s.%s", resultDatabase, resultTable));
    }

    String sql = "";
    SqlDialect dialect = new HiveSqlDialect(SqlDialect.EMPTY_CONTEXT);
    if (info.getAqpNode() instanceof SqlWith) {
      List<String> withItems = new ArrayList<>();
      SqlWith with = (SqlWith) info.getAqpNode();
      for (SqlNode node : with.withList) {
        String withSql = node.toSqlString(dialect).toString();
        withSql = withSql.replaceAll("AS SELECT", "AS (SELECT");
        withSql += ")";
        withItems.add(withSql);
      }
      String query = with.body.toSqlString(dialect).toString();
      sql =
          String.format(
              "CREATE TABLE %s.%s STORED AS parquet AS WITH %s %s",
              resultDatabase, resultTable, Joiner.on(",").join(withItems), query);
    } else {
      String query = info.getAqpNode().toSqlString(dialect).toString();
      sql =
          String.format(
              "CREATE TABLE %s.%s STORED AS parquet AS %s", resultDatabase, resultTable, query);
    }
    sql = sql.replaceAll("ASYMMETRIC", "");
    sql = sql.replaceAll("LIMIT 100", "");
    sql = sql.replaceAll("limit 100", "");
    sql = sql.replaceAll("ROWS ONLY", "");
    sql = sql.replaceAll("FETCH NEXT", "LIMIT");

    if (args.isMeasureTime()) {
      this.clearCache(args.getClearCacheScript());
    }
    Stopwatch watch = Stopwatch.createStarted();
    this.execute(sql);
    watch.stop();
    return watch.elapsed(TimeUnit.MILLISECONDS);
  }

  @Override
  public Set<Table> getAllTableAndColumns(String database) throws SQLException {
    String sql = String.format("SHOW TABLES IN %s", database);
    ResultSet rs = this.executeQuery(sql);

    Set<Table> tables = new HashSet<>();
    // get tables
    while (rs.next()) {
      String table = rs.getString(1);
      tables.add(new Table(table));
    }

    // get columns for each table
    for (Table table : tables) {
      String tableName = table.getName();
      sql = String.format("DESCRIBE %s.%s", database, tableName);
      rs = this.executeQuery(sql);
      while (rs.next()) {
        String name = rs.getString(1);
        String type = rs.getString(2);
        Column c = new Column(table, name, type);
        table.addColumn(c);
      }
    }

    return tables;
  }

  @Override
  public void createDatabaseIfNotExists(String database) throws SQLException {
    String sql = String.format("CREATE DATABASE IF NOT EXISTS %s", database);
    this.execute(sql);
  }

  @Override
  public String getRandomFunction() {
    int randomNum = ThreadLocalRandom.current().nextInt(0, (int) 1e6);
    // 1. unix_timestamp() prevents the same random numbers are generated for different column
    // especially when "create table as select" is used.
    // 2. adding a random number to the timestamp prevents the same random numbers are generated
    // for different columns.
    return String.format("rand(unix_timestamp()+%d)", randomNum);
  }

  @Override
  public String getCurrentTimestamp() {
    return "current_timestamp()";
  }

  @Override
  public void createUniformSample(String targetDatabase, String sourceDatabase, UniformSample s)
      throws SQLException {
    String sampleTableName = s.getSampleTableName();
    if (checkTableExists(targetDatabase, sampleTableName)) {
      Logger.info("Uniform sample '{}.{}' already exists", targetDatabase, sampleTableName);
      return;
    }

    String sql =
        String.format(
            "CREATE TABLE IF NOT EXISTS %s.%s STORED AS PARQUET "
                + "AS SELECT * FROM %s.%s WHERE %s < %.4f",
            targetDatabase,
            sampleTableName,
            sourceDatabase,
            s.getTable().getName(),
            this.getRandomFunction(),
            s.getRatio());

    // create uniform sample
    this.execute(sql);

    // compute stats for the sample
    this.execute(String.format("COMPUTE STATS %s.%s", targetDatabase, sampleTableName));
  }

  @Override
  public void constructPrejoinWithUniformSample(
      String targetDatabase, String sourceDatabase, Prejoin p, double ratio) throws SQLException {

    InnocentMeta meta = InnocentMeta.getInstance(targetDatabase, this);
    p.setSampleRatio(ratio);
    String metaString = p.getJsonStringForJoinSet();
    String prejoinTableName = p.getPrejoinTableName();
    if (checkTableExists(targetDatabase, prejoinTableName)) {
      Logger.info("Prejoin '{}.{}' already exists", targetDatabase, prejoinTableName);
      return;
    }

    UniformSample uniformSample = new UniformSample(p.getFactTable(), ratio);
    // construct uniform sample if not exists
    this.createUniformSample(targetDatabase, sourceDatabase, uniformSample);

    String factTableSampleName = uniformSample.getSampleTableName();
    Set<FactDimensionJoin> joinSet = p.getJoinSet();

    List<String> tableList = new ArrayList<>();
    List<String> joinExpressions = new ArrayList<>();
    tableList.add(targetDatabase + "." + factTableSampleName);

    for (FactDimensionJoin j : joinSet) {
      tableList.add(sourceDatabase + "." + j.getDimensionTable().getName());
      for (UnorderedPair<Column> joinPair : j.getJoinPairs()) {
        joinExpressions.add(joinPair.getLeft().getName() + " = " + joinPair.getRight().getName());
      }
    }

    String sql =
        String.format(
            "CREATE TABLE IF NOT EXISTS %s.%s STORED AS PARQUET AS SELECT * FROM %s WHERE %s",
            targetDatabase,
            prejoinTableName,
            Joiner.on(",").join(tableList),
            Joiner.on(" AND ").join(joinExpressions));

    // create prejoin with uniform sample
    this.execute(sql);

    // compute stats for the prejoin
    this.execute(String.format("COMPUTE STATS %s.%s", targetDatabase, prejoinTableName));

    // add metadata
    meta.put(prejoinTableName, metaString);
  }
}
