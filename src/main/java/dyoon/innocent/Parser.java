package dyoon.innocent;

import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.dialect.HiveSqlDialect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.util.SqlShuttle;
import org.apache.calcite.sql.util.SqlString;

/** Created by Dong Young Yoon on 10/19/18. */
public class Parser {

  public QueryVisitor parse(String sql) throws SqlParseException, ClassNotFoundException {
    Class.forName("org.apache.calcite.jdbc.Driver");
    SqlParser sqlParser = SqlParser.create(sql);
    SqlNode node = sqlParser.parseQuery();
    SqlShuttle s = new SqlShuttle();

    SqlDialect dialect = new HiveSqlDialect(SqlDialect.EMPTY_CONTEXT);

    QueryVisitor visitor = new QueryVisitor();

    node.accept(visitor);

    if (node instanceof SqlOrderBy) {
      // get rid of LIMIT
      SqlOrderBy orderBy = (SqlOrderBy) node;
      node =
          new SqlOrderBy(
              node.getParserPosition(), orderBy.query, orderBy.orderList, orderBy.offset, null);
    }

    SqlString sqlString = node.toSqlString(dialect);

    return visitor;
  }
}
