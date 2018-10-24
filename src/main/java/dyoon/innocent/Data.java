package dyoon.innocent;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Created by Dong Young Yoon on 10/23/18. */
public class Data {

  private static final Set<String> FACT_TABLES =
      new HashSet<>(
          Arrays.asList(
              "store_sales",
              "store_returns",
              "catalog_sales",
              "catalog_returns",
              "web_sales",
              "web_returns",
              "inventory"));

  private Map<String, List<String>> tableToColumns;
  private Map<String, String> columnToTable;
  private Map<String, Integer> columnFrequency;

  public Data() {
    tableToColumns = new HashMap<>();
    columnToTable = new HashMap<>();
    columnFrequency = new HashMap<>();
  }

  public void addTable(String table) {
    if (!tableToColumns.containsKey(table)) {
      tableToColumns.put(table, new ArrayList<>());
    }
  }

  public void addColumnToTable(String table, String column) {
    tableToColumns.get(table).add(column);
    columnToTable.put(column, table);
  }

  public void incrementFreq(String column) {
    if (!columnFrequency.containsKey(column)) {
      columnFrequency.put(column, 1);
    } else {
      columnFrequency.put(column, columnFrequency.get(column) + 1);
    }
  }

  public Map<String, Integer> getColumnFrequency() {
    return columnFrequency;
  }

  public boolean isColumnFromFactTable(String column) {
    String table = columnToTable.get(column);
    return FACT_TABLES.contains(table);
  }
}
