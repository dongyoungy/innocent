package dyoon.innocent.query;

import dyoon.innocent.Utils;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.util.SqlShuttle;

import java.util.Collection;

/**
 * Created by Dong Young Yoon on 11/19/18.
 *
 * <p>Check whether a select item contains alias(es)
 */
public class AliasDetector extends SqlShuttle {

  private Collection<SqlIdentifier> aliasList;
  private boolean isAliasFound;

  public AliasDetector(Collection<SqlIdentifier> aliasList) {
    this.aliasList = aliasList;
    this.isAliasFound = false;
  }

  public void reset() {
    this.isAliasFound = false;
  }

  public boolean isAliasFound() {
    return isAliasFound;
  }

  @Override
  public SqlNode visit(SqlIdentifier id) {
    if (Utils.containsLastName(aliasList, id)) {
      this.isAliasFound = true;
    }
    return super.visit(id);
  }
}
