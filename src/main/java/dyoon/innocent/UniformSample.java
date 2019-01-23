package dyoon.innocent;

import dyoon.innocent.data.Table;

/** Created by Dong Young Yoon on 2019-01-15. */
public class UniformSample extends Sample {

  private double ratio;

  public UniformSample(Table table, double ratio) {
    this.table = table;
    this.ratio = ratio;
    this.id = 1;
  }

  public UniformSample(Table table, double ratio, int id) {
    this.table = table;
    this.ratio = ratio;
    this.id = id;
  }

  public double getRatio() {
    return ratio;
  }

  @Override
  public String getSampleTableName() {
    String name = String.format("%s___ut___%.4f___%d", table, ratio, id);
    return name.replaceAll("\\.", "_");
  }
}
