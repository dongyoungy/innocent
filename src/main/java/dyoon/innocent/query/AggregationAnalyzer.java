package dyoon.innocent.query;

import dyoon.innocent.Utils;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlAsOperator;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.util.SqlShuttle;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Dong Young Yoon on 11/4/18.
 *
 * <p>Analyzes an individual select item for: 1) outer-most agg function 2) first agg function that
 * takes a column from sample table as an argument.
 */
public class AggregationAnalyzer extends SqlShuttle {

  private boolean isFirstAggfunction;
  private boolean isFirstAggFunctionWithSampleColumn;

  private SqlIdentifier alias;
  private SqlOperator firstAggOp;
  private SqlOperator firstAggWithSampleColumnOp;
  private SqlIdentifier aggSourceColumn;
  private SqlNode aggSource;
  private SqlBasicCall innerMostAgg;

  private List<String> sampleTableColumns;

  public AggregationAnalyzer(List<String> sampleTableColumns) {
    this.sampleTableColumns = new ArrayList<>(sampleTableColumns);
    this.isFirstAggfunction = true;
    this.isFirstAggFunctionWithSampleColumn = true;
  }

  public SqlOperator getFirstAggOp() {
    return firstAggOp;
  }

  public SqlOperator getFirstAggWithSampleColumnOp() {
    return firstAggWithSampleColumnOp;
  }

  public SqlIdentifier getAggSourceColumn() {
    return aggSourceColumn;
  }

  public SqlIdentifier getAlias() {
    return alias;
  }

  public SqlNode getAggSource() {
    return aggSource;
  }

  public SqlBasicCall getInnerMostAgg() {
    return innerMostAgg;
  }

  @Override
  public SqlNode visit(SqlCall call) {
    if (call instanceof SqlBasicCall) {
      SqlBasicCall bc = (SqlBasicCall) call;
      SqlOperator op = bc.getOperator();

      if (op instanceof SqlAsOperator) {
        this.alias = (SqlIdentifier) bc.operands[1];
      } else if (op instanceof SqlAggFunction) {
        if (isFirstAggfunction) {
          this.firstAggOp = op;
          this.isFirstAggfunction = false;
          if (bc.operands.length == 1) {
            this.aggSource = bc.operands[0];
          }
        }

        InnerMostAggFinder finder = new InnerMostAggFinder(sampleTableColumns);
        bc.operands[0].accept(finder);
        // check whether operand is a column from the sample table
        if (bc.operands.length == 1 && finder.isInnerMostAgg()) {
          this.innerMostAgg = bc;
          this.firstAggWithSampleColumnOp = op;

          //          SqlIdentifier id = (SqlIdentifier) bc.operands[0];
          //          String idStr = id.names.get(id.names.size() - 1);
          //          if (Utils.containsIgnoreCase(idStr, sampleTableColumns)
          //              && isFirstAggFunctionWithSampleColumn) {
          //            this.aggSourceColumn = id;
          //            this.firstAggWithSampleColumnOp = op;
          //            this.isFirstAggFunctionWithSampleColumn = false;
          //          }
        }
      }
    }
    return super.visit(call);
  }

  // check whether the current item is in the inner most aggregation.
  class InnerMostAggFinder extends SqlShuttle {
    boolean isInnerMostAgg;
    List<String> sampleTableColumns;

    public InnerMostAggFinder(List<String> sampleTableColumns) {
      this.isInnerMostAgg = false;
      this.sampleTableColumns = sampleTableColumns;
    }

    public boolean isInnerMostAgg() {
      return isInnerMostAgg;
    }

    @Override
    public SqlNode visit(SqlIdentifier id) {
      String idStr = id.names.get(id.names.size() - 1);
      if (Utils.containsIgnoreCase(idStr, sampleTableColumns) || idStr.isEmpty()) {
        this.isInnerMostAgg = true;
      }
      return id;
    }

    @Override
    public SqlNode visit(SqlCall call) {
      if (call instanceof SqlBasicCall) {
        SqlBasicCall bc = (SqlBasicCall) call;
        SqlOperator op = bc.getOperator();
        if (op instanceof SqlAggFunction) {
          this.isInnerMostAgg = false;
          return call;
        }
      }
      return super.visit(call);
    }
  }
}
