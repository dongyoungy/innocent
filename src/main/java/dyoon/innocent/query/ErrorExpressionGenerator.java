package dyoon.innocent.query;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import dyoon.innocent.Utils;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlBinaryOperator;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOverOperator;
import org.apache.calcite.sql.fun.SqlAvgAggFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.fun.SqlSumAggFunction;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlShuttle;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Created by Dong Young Yoon on 11/20/18.
 *
 * <p>For a given select item, construct corresponding error expression
 */
public class ErrorExpressionGenerator extends SqlShuttle {

  private Set<SqlOperator> aggInOverOperatorSet;
  private Set<SqlNode> doNotModifySet;
  private BiMap<SqlIdentifier, SqlIdentifier> sourceToErrorMap;
  private BiMap<SqlNode, SqlNode> exprToErrorMap;
  private AliasDetector detector;

  public ErrorExpressionGenerator(Map<SqlIdentifier, SqlIdentifier> sourceToErrorMap) {
    this.sourceToErrorMap = HashBiMap.create();
    this.sourceToErrorMap.putAll(sourceToErrorMap);
    this.exprToErrorMap = HashBiMap.create();
    this.detector = new AliasDetector(sourceToErrorMap.keySet());
    this.doNotModifySet = new HashSet<>();
    this.aggInOverOperatorSet = new HashSet<>();
  }

  private boolean isError(SqlIdentifier id) {
    return sourceToErrorMap.values().contains(id);
  }

  public Set<SqlNode> getDoNotModifySet() {
    return doNotModifySet;
  }

  @Override
  public SqlNode visit(SqlCall call) {

    // Mark window function first.
    if (call instanceof SqlBasicCall) {
      SqlBasicCall bc = (SqlBasicCall) call;
      SqlOperator op = bc.getOperator();

      if (op instanceof SqlOverOperator) {
        // mark agg operator in the window function for future processing
        SqlBasicCall operand = (SqlBasicCall) bc.operands[0];
        SqlOperator aggOp = operand.getOperator();
        this.aggInOverOperatorSet.add(aggOp);
      }
    }

    SqlNode node = super.visit(call);
    if (node instanceof SqlBasicCall) {
      SqlBasicCall bc = (SqlBasicCall) node;
      SqlOperator op = bc.getOperator();
      if (op instanceof SqlBinaryOperator) {
        SqlBinaryOperator binaryOp = (SqlBinaryOperator) op;
        String opName = binaryOp.getName();
        SqlNode left = bc.operands[0];
        SqlNode right = bc.operands[1];
        if (opName.equals("+") || opName.equals("-")) {
          return handlePlusMinus(bc, left, right);
        } else if (opName.equals("*") || opName.equals("/")) {
          return handleMultiplyDivide(bc, left, right);
        } else if (opName.equals("OVER")) {
          // if window function
          return handleAggWithOver(bc);
        }
      } else if (op instanceof SqlSumAggFunction) {
        if (!aggInOverOperatorSet.contains(op)) {
          SqlNode operand = bc.operands[0];
          return handleSum(bc, operand);
        }
      } else if (op instanceof SqlAvgAggFunction) {
        if (!aggInOverOperatorSet.contains(op)) {
          SqlNode operand = bc.operands[0];
          return handleAverage(bc, operand);
        }
      }
    }
    return node;
  }

  private SqlNode handleAggWithOver(SqlBasicCall over) {
    SqlBasicCall aggNode = (SqlBasicCall) over.operands[0];
    SqlOperator aggOp = aggNode.getOperator();

    if (aggOp instanceof SqlSumAggFunction) {
      return handleSumWithOver(over, aggNode);
    } else if (aggOp instanceof SqlAvgAggFunction) {
      return handleAverageWithOver(over, aggNode);
    } else return over;
  }

  private SqlNode handleSumWithOver(SqlBasicCall over, SqlBasicCall agg) {
    SqlNode operand = agg.operands[0];
    boolean hasError = this.checkError(operand);
    if (hasError) {
      over.setOperand(0, Utils.sum(Utils.pow(operand, 2)));
    }
    return Utils.sqrt(over);
  }

  private SqlNode handleAverageWithOver(SqlBasicCall over, SqlBasicCall agg) {
    SqlNode operand = agg.operands[0];
    SqlNode overRight = over.operands[1];
    boolean hasError = this.checkError(operand);
    if (hasError) {
      over.setOperand(0, Utils.sum(Utils.pow(operand, 2)));
    }
    over = Utils.sqrt(over);
    // now divide it with count(*) over ...
    SqlBasicCall count =
        new SqlBasicCall(SqlStdOperatorTable.COUNT, new SqlNode[] {}, SqlParserPos.ZERO);
    SqlNode countOver =
        new SqlBasicCall(
            SqlStdOperatorTable.OVER, new SqlNode[] {count, overRight}, SqlParserPos.ZERO);
    return Utils.divide(over, countOver);
  }

  private boolean checkError(SqlNode node) {
    detector.reset();
    node.accept(detector);
    return detector.isAliasFound();
  }

  private Pair<Boolean, Boolean> checkBinaryOperandWithError(SqlNode left, SqlNode right) {
    detector.reset();
    left.accept(detector);
    boolean leftHasError = detector.isAliasFound();
    detector.reset();
    right.accept(detector);
    boolean rightHasError = detector.isAliasFound();
    return ImmutablePair.of(leftHasError, rightHasError);
  }

  private SqlNode handleSum(SqlBasicCall original, SqlNode node) {
    boolean hasError = this.checkError(node);
    if (hasError) {
      return Utils.sqrt(Utils.sum(Utils.pow(node, 2)));
    } else {
      return original;
    }
  }

  private SqlNode handleAverage(SqlBasicCall original, SqlNode node) {
    boolean hasError = this.checkError(node);
    if (hasError) {
      SqlBasicCall count =
          new SqlBasicCall(SqlStdOperatorTable.COUNT, new SqlNode[] {}, SqlParserPos.ZERO);
      return Utils.divide(Utils.sqrt(Utils.sum(Utils.pow(node, 2))), count);
    } else {
      return original;
    }
  }

  // This method will create error term for A + B (or A - B).
  // It transforms the original expression in the following order:
  // (1) A + B --> (2) sqrt(A^2 + B^2) --> (3) sqrt(A_err^2 + B_err^2)
  //
  // This method is responsible for conversion from (1) to (2)
  // The conversion from (2) to (3) is done by SourceToErrorSubstitutor class.
  private SqlNode handlePlusMinus(SqlBasicCall original, SqlNode left, SqlNode right) {
    Pair<Boolean, Boolean> pair = this.checkBinaryOperandWithError(left, right);
    boolean leftHasError = pair.getLeft();
    boolean rightHasError = pair.getRight();

    if (leftHasError && rightHasError) {
      TreeCloner cloner = new TreeCloner();
      SqlNode newLeft = left.accept(cloner);
      SqlNode newRight = right.accept(cloner);
      SqlNode newOrig =
          new SqlBasicCall(
              original.getOperator(), new SqlNode[] {newLeft, newRight}, SqlParserPos.ZERO);
      doNotModifySet.add(newOrig);

      // returns sqrt(l^2 + r^2)
      SqlNode error = Utils.sqrt(Utils.plus(Utils.pow(left, 2), Utils.pow(right, 2)));
      exprToErrorMap.put(newOrig, error);
      return error;
    } else if (leftHasError) {
      return left;
    } else if (rightHasError) {
      return right;
    } else {
      return original;
    }
  }

  // This method will create error term for A * B (or A / B).
  // It transforms the original expression in the following order:
  // (1) A * B --> (2) A * B * sqrt((A/A)^2 + (B/B)^2) -->
  // (3) A * B * sqrt((A_err/A)^2 + (B_err/B)^2)
  //
  // This method is responsible for conversion from (1) to (2)
  // The conversion from (2) to (3) is done by SourceToErrorSubstitutor class.
  //
  // SourceToErrorSubstitutor basically replaces an original term with its error term
  // (i.e., A --> A_err).
  //
  // This method marks the term that should not be changed (i.e., A needs to stay as A) by
  // putting it into the set 'doNotModifySet'.
  private SqlNode handleMultiplyDivide(SqlBasicCall original, SqlNode left, SqlNode right) {
    Pair<Boolean, Boolean> pair = this.checkBinaryOperandWithError(left, right);
    boolean leftHasError = pair.getLeft();
    boolean rightHasError = pair.getRight();

    if (leftHasError && rightHasError) {
      TreeCloner cloner = new TreeCloner();
      SqlNode newLeft, newRight;
      if (exprToErrorMap.inverse().containsKey(left)) {
        newLeft = exprToErrorMap.inverse().get(left);
      } else {
        newLeft = left.accept(cloner);
        doNotModifySet.add(newLeft);
      }
      if (exprToErrorMap.inverse().containsKey(right)) {
        newRight = exprToErrorMap.inverse().get(right);
      } else {
        newRight = right.accept(cloner);
        doNotModifySet.add(newRight);
      }
      SqlNode newOrig =
          new SqlBasicCall(
              original.getOperator(), new SqlNode[] {newLeft, newRight}, SqlParserPos.ZERO);
      doNotModifySet.add(newOrig);
      // returns l_orig * r_orig * sqrt((l/l_orig)^2 + (r/r_orig)^2)
      SqlNode error =
          Utils.multiply(
              newLeft,
              Utils.multiply(
                  newRight,
                  Utils.sqrt(
                      Utils.plus(
                          Utils.pow(Utils.divide(left, newLeft), 2),
                          Utils.pow(Utils.divide(right, newRight), 2)))));
      exprToErrorMap.put(newOrig, error);
      return error;
    } else {
      return original;
    }
  }
}
