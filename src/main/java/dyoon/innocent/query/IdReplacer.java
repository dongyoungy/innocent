package dyoon.innocent.query;

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlShuttle;

/**
 * Created by Dong Young Yoon on 2019-02-02.
 *
 * <p>Replace all SqlIdentifier found in a tree
 */
public class IdReplacer extends SqlShuttle {
  private SqlIdentifier toId;
  private SqlIdentifier fromId; // optional

  public IdReplacer(SqlIdentifier toId) {
    this.toId = toId;
  }

  public IdReplacer(SqlIdentifier toId, SqlIdentifier fromId) {
    this.toId = toId;
    this.fromId = fromId;
  }

  @Override
  public SqlNode visit(SqlIdentifier id) {

    if (fromId != null && !id.equals(fromId)) {
      return id;
    }

    SqlIdentifier newId = (SqlIdentifier) id.clone(SqlParserPos.ZERO);
    if (toId.names.size() >= id.names.size()) {
      return toId;
    } else {
      int idx = 1;
      for (int i = toId.names.size() - 1; i >= 0; --i) {
        newId = newId.setName(newId.names.size() - idx, toId.names.get(i));
        ++idx;
      }
      return newId;
    }
  }
}
