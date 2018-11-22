package dyoon.innocent;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import dyoon.innocent.query.ErrorExpressionGenerator;
import dyoon.innocent.query.SourceToErrorSubstitutor;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;

import java.util.Map;

/** Created by Dong Young Yoon on 11/20/18. */
public class ErrorColumnGenerator {

  private BiMap<SqlIdentifier, SqlIdentifier> sourceToErrorMap;

  public ErrorColumnGenerator(BiMap<SqlIdentifier, SqlIdentifier> sourceToErrorMap) {
    this.sourceToErrorMap = sourceToErrorMap;
  }

  public ErrorColumnGenerator(Map<SqlIdentifier, SqlIdentifier> sourceToErrorMap) {
    this.sourceToErrorMap = HashBiMap.create();
    this.sourceToErrorMap.putAll(sourceToErrorMap);
  }

  public SqlNode generateErrorColumn(SqlNode orig) {
    ErrorExpressionGenerator exprGen = new ErrorExpressionGenerator(sourceToErrorMap);
    SourceToErrorSubstitutor substitutor = new SourceToErrorSubstitutor(sourceToErrorMap);

    SqlNode node = orig.accept(exprGen);
    substitutor.setDoNotModifySet(exprGen.getDoNotModifySet());
    node = node.accept(substitutor);

    return node;
  }
}
