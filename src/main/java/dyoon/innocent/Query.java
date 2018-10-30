package dyoon.innocent;

/** Created by Dong Young Yoon on 10/30/18. */
public class Query {

  private String id;
  private String query;
  private String aqpQuery;

  public Query(String id, String query) {
    this.id = id;
    this.query = query;
  }

  public String getId() {
    return id;
  }

  public String getQuery() {
    return query;
  }

  public String getAqpQuery() {
    return aqpQuery;
  }

  public void setAqpQuery(String aqpQuery) {
    this.aqpQuery = aqpQuery;
  }

  public String getResultTableName() {
    return id + "_orig";
  }
}
