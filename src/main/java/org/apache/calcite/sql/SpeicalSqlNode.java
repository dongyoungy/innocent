package org.apache.calcite.sql;

import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlVisitor;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.util.Litmus;

/** Created by Dong Young Yoon on 11/20/18. */
public abstract class SpeicalSqlNode extends SqlNode {

  public SpeicalSqlNode(SqlParserPos pos) {
    super(pos);
  }

  @Override
  public SqlNode clone(SqlParserPos sqlParserPos) {
    return SqlNode.clone(this);
  }

  @Override
  public void unparse(SqlWriter sqlWriter, int i, int i1) {
    this.unparse(sqlWriter, i, i1);
  }

  @Override
  public void validate(SqlValidator sqlValidator, SqlValidatorScope sqlValidatorScope) {}

  @Override
  public <R> R accept(SqlVisitor<R> sqlVisitor) {
    return null;
  }

  @Override
  public boolean equalsDeep(SqlNode sqlNode, Litmus litmus) {
    return false;
  }
}
