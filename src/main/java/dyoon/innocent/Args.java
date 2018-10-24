package dyoon.innocent;

import com.beust.jcommander.Parameter;

/** Created by Dong Young Yoon on 10/23/18. */
public class Args {
  @Parameter(names = "--create", description = "Create samples at the end")
  private boolean create = false;

  @Parameter(names = "--help", help = true)
  private boolean help = false;

  @Parameter(
      names = "--top-n-col",
      description = "Consider top-N frequently appearing columns for each fact table")
  private int topNColumns = 3;

  @Parameter(
      names = "--max-col-per-sample",
      description = "Maximum number of columns used in each sample")
  private int maxColPerSample = 2;

  @Parameter(
      names = "--min-rows",
      description = "Comma separated values for minimum rows for stratified samples")
  private String minRows = "";

  @Parameter(names = "--query-dir", description = "directory where queries are")
  private String queryDir = "/Users/dyoon/work/impala-tpcds-kit/queries";

  @Parameter(
      names = {"-d", "--database"},
      description = "database/schema")
  private String database = "tpcds_500_parquet";

  @Parameter(
      names = {"-h", "--host"},
      description = "host")
  private String host = "c220g5-110932.wisc.cloudlab.us:21050";

  public boolean isCreate() {
    return create;
  }

  public boolean isHelp() {
    return help;
  }

  public int getTopNColumns() {
    return topNColumns;
  }

  public int getMaxColPerSample() {
    return maxColPerSample;
  }

  public String getMinRows() {
    return minRows;
  }

  public String getQueryDir() {
    return queryDir;
  }

  public String getDatabase() {
    return database;
  }

  public String getHost() {
    return host;
  }
}
