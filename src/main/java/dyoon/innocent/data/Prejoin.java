package dyoon.innocent.data;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import dyoon.innocent.Query;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

/** Created by Dong Young Yoon on 2019-01-08. */
@JsonIgnoreProperties(ignoreUnknown = true)
public class Prejoin {
  private Table factTable;
  private Set<FactDimensionJoin> joinSet;
  private Set<Query> queries;
  private double sampleRatio;

  private Prejoin() {
    // for JSON
  }

  public Prejoin(Table factTable) {
    this.factTable = factTable;
    this.joinSet = new HashSet<>();
    this.queries = new HashSet<>();
    this.sampleRatio = 1.0;
  }

  public double getSampleRatio() {
    return sampleRatio;
  }

  public void setSampleRatio(double sampleRatio) {
    this.sampleRatio = sampleRatio;
  }

  public Table getFactTable() {
    return factTable;
  }

  public Set<Query> getQueries() {
    return queries;
  }

  public Set<FactDimensionJoin> getJoinSet() {
    return joinSet;
  }

  public void addJoin(FactDimensionJoin join) {
    joinSet.add(join);
  }

  public boolean containDimension(FactDimensionJoin j) {
    for (FactDimensionJoin join : joinSet) {
      if (join.getDimensionTable().equals(j.getDimensionTable())) {
        return true;
      }
    }
    return false;
  }

  public boolean contains(Table fact, Table dim, Set<UnorderedPair<Column>> joinColumnPairs) {
    for (FactDimensionJoin join : joinSet) {
      if (join.getFactTable().equals(fact)
          && join.getDimensionTable().equals(dim)
          && join.getJoinPairs().containsAll(joinColumnPairs)) {
        return true;
      }
    }
    return false;
  }

  public boolean containSameDimensionWithDiffKey(FactDimensionJoin j) {
    for (FactDimensionJoin join : joinSet) {
      if (join.getDimensionTable().equals(j.getDimensionTable())
          && !join.getJoinPairs().equals(j.getJoinPairs())) {
        return true;
      }
    }
    return false;
  }

  public void addQuery(Query q) {
    queries.add(q);
  }

  public String getJsonStringForJoinSet() {
    ObjectMapper objectMapper = new ObjectMapper();
    objectMapper.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);
    String jsonString;
    Set<UnorderedPair<Column>> pairs = new HashSet<>();
    for (FactDimensionJoin join : joinSet) {
      pairs.addAll(join.getJoinPairs());
    }
    try {
      jsonString = objectMapper.writeValueAsString(pairs);
      return jsonString;
    } catch (JsonProcessingException e) {
      e.printStackTrace();
      return null;
    }
  }

  public static Set<UnorderedPair<Column>> getJoinSetfromJsonString(String jsonString) {
    ObjectMapper objectMapper = new ObjectMapper();
    objectMapper.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);
    objectMapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
    try {
      Set<UnorderedPair<Column>> set =
          objectMapper.readValue(
              jsonString,
              objectMapper
                  .getTypeFactory()
                  .constructCollectionType(
                      Set.class,
                      objectMapper
                          .getTypeFactory()
                          .constructParametricType(UnorderedPair.class, Column.class)));
      return set;
    } catch (IOException e) {
      e.printStackTrace();
      return null;
    }
  }

  public String getPrejoinTableName() {
    SortedSet<String> tableNames = new TreeSet<>();
    for (FactDimensionJoin join : joinSet) {
      tableNames.add(join.getDimensionTable().getName());
    }
    String name =
        String.format(
            "prejoin___%s___%.4f___%d___%d",
            factTable.getName(),
            sampleRatio,
            Math.abs(tableNames.hashCode()),
            //            Joiner.on("__").join(tableNames),
            Math.abs(joinSet.hashCode()));
    return name.replaceAll("\\.", "_");
  }

  @Override
  public int hashCode() {
    int hash = 0;
    for (FactDimensionJoin join : joinSet) {
      hash += join.hashCode();
    }

    return hash;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof Prejoin) {
      Prejoin other = (Prejoin) obj;
      return factTable.equals(other.factTable) && joinSet.equals(other.joinSet);
    }
    return false;
  }
}
