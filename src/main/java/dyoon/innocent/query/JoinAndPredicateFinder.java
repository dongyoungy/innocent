package dyoon.innocent.query;

import dyoon.innocent.Utils;
import dyoon.innocent.data.Column;
import dyoon.innocent.data.Join;
import dyoon.innocent.data.Predicate;
import dyoon.innocent.data.Table;
import org.apache.calcite.sql.SqlAsOperator;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlBinaryOperator;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.util.SqlShuttle;
import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.apache.commons.lang3.tuple.Triple;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** Created by Dong Young Yoon on 2018-12-15. */
public class JoinAndPredicateFinder extends SqlShuttle {

  private Set<Join> joinSet;
  private Set<Table> allTables;
  private List<Column> allColumns;

  public JoinAndPredicateFinder(Set<Table> allTables, List<Column> allColumns) {
    this.joinSet = new HashSet<>();
    this.allTables = allTables;
    this.allColumns = allColumns;
  }

  public Set<Join> getJoinSet() {
    return joinSet;
  }

  @Override
  public SqlNode visit(SqlCall call) {
    if (call instanceof SqlSelect) {
      SqlSelect select = (SqlSelect) call;
      TableExtractor extractor = new TableExtractor();
      PredicateExtractor pe = new PredicateExtractor();
      if (select.getFrom() != null) {
        select.getFrom().accept(extractor);
      }
      if (select.getWhere() != null) {
        select.getWhere().accept(pe);
      }
      Set<Table> tables = extractor.getTables();
      Set<Predicate> predicates = pe.getPredicates();
      if (!tables.isEmpty() && !predicates.isEmpty()) {
        joinSet.add(new Join(tables, predicates));
      }
    }
    return super.visit(call);
  }

  class TableExtractor extends SqlShuttle {

    Set<Table> tables;

    public TableExtractor() {
      this.tables = new HashSet<>();
    }

    public Set<Table> getTables() {
      return tables;
    }

    @Override
    public SqlNode visit(SqlIdentifier id) {
      Table t = Utils.findTableByName(allTables, id.names.get(id.names.size() - 1));
      if (t != null) tables.add(t);
      return super.visit(id);
    }

    @Override
    public SqlNode visit(SqlCall call) {
      if (call instanceof SqlBasicCall) {
        SqlBasicCall bc = (SqlBasicCall) call;
        SqlOperator op = bc.getOperator();
        if (op instanceof SqlAsOperator) {
          bc.operands[0].accept(this);
        } else {
          return super.visit(call);
        }
      } else {
        return super.visit(call);
      }
      return call;
    }
  }

  class PredicateExtractor extends SqlShuttle {
    private Set<Predicate> predicates;

    public PredicateExtractor() {
      this.predicates = new HashSet<>();
    }

    public Set<Predicate> getPredicates() {
      return predicates;
    }

    @Override
    public SqlNode visit(SqlCall call) {
      if (call instanceof SqlSelect) {
        // ignore select in WHERE
        return call;
      } else if (call instanceof SqlBasicCall) {
        SqlBasicCall bc = (SqlBasicCall) call;
        SqlOperator op = bc.getOperator();
        if (op instanceof SqlBinaryOperator) {
          Triple<SqlIdentifier, Double, Boolean> predicate = this.getBinaryPredicate(bc);
          if (predicate != null) {
            String opName = op.getName();
            SqlIdentifier column = predicate.getLeft();
            Double value = predicate.getMiddle();
            boolean isColumnOnLeft = predicate.getRight();
            switch (opName) {
              case "=":
                {
                  Predicate p = Utils.buildEqualPredicate(allColumns, column, value);
                  if (p != null) this.predicates.add(p);
                  break;
                }
              case "<":
                {
                  Predicate p =
                      (isColumnOnLeft
                          ? Utils.buildLessThanPredicate(allColumns, column, value)
                          : Utils.buildGreaterThanPredicate(allColumns, column, value));
                  if (p != null) this.predicates.add(p);
                  break;
                }
              case "<=":
                {
                  Predicate p =
                      (isColumnOnLeft
                          ? Utils.buildLessThanEqualPredicate(allColumns, column, value)
                          : Utils.buildGreaterThanEqualPredicate(allColumns, column, value));
                  if (p != null) this.predicates.add(p);
                  break;
                }
              case ">":
                {
                  Predicate p =
                      (isColumnOnLeft
                          ? Utils.buildGreaterThanPredicate(allColumns, column, value)
                          : Utils.buildLessThanPredicate(allColumns, column, value));
                  if (p != null) this.predicates.add(p);
                  break;
                }
              case ">=":
                {
                  Predicate p =
                      (isColumnOnLeft
                          ? Utils.buildGreaterThanEqualPredicate(allColumns, column, value)
                          : Utils.buildLessThanEqualPredicate(allColumns, column, value));
                  if (p != null) this.predicates.add(p);
                  break;
                }
              default:
                break;
            }
          }
        }
      }
      return super.visit(call);
    }

    // returns {column, value, isColumnOnLeft} for binary predicate
    private Triple<SqlIdentifier, Double, Boolean> getBinaryPredicate(SqlBasicCall bc) {
      if (bc.operands.length == 2) {
        if (bc.operands[0] instanceof SqlIdentifier
            && bc.operands[1] instanceof SqlNumericLiteral) {
          SqlIdentifier col = (SqlIdentifier) bc.operands[0];
          SqlNumericLiteral num = (SqlNumericLiteral) bc.operands[1];
          Double val = num.bigDecimalValue().doubleValue();
          return ImmutableTriple.of(col, val, true);
        } else if (bc.operands[1] instanceof SqlIdentifier
            && bc.operands[0] instanceof SqlNumericLiteral) {
          SqlIdentifier col = (SqlIdentifier) bc.operands[1];
          SqlNumericLiteral num = (SqlNumericLiteral) bc.operands[0];
          Double val = num.bigDecimalValue().doubleValue();
          return ImmutableTriple.of(col, val, false);
        }
      }
      return null;
    }
  }
}
