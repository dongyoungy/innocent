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
}
