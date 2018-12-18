package dyoon.innocent.lp;

import dyoon.innocent.Query;
import dyoon.innocent.data.PartitionSpace;
import org.chocosolver.solver.Model;
import org.chocosolver.solver.Solution;
import org.chocosolver.solver.variables.IntVar;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Created by Dong Young Yoon on 2018-12-17. */
public class ILPSolver {

  public static void solveForBestColumnForPartition(List<PartitionSpace> partitionSpaces, int k) {
    Model m = new Model("test");
    Set<String> querySet = new HashSet<>();
    Map<String, List<IntVar>> qToPs = new HashMap<>();
    Map<String, IntVar> qMap = new HashMap<>();
    int i = 0;
    IntVar[] psVar = new IntVar[partitionSpaces.size()];
    for (PartitionSpace space : partitionSpaces) {
      String ps = String.format("ps_%d", i);
      psVar[i] = m.intVar(ps, 0, 1);
      for (Query query : space.getQueries()) {
        String q = String.format("q_%s", query.getId());
        querySet.add(q);
        if (!qToPs.containsKey(q)) {
          qToPs.put(q, new ArrayList<>());
        }
        List<IntVar> psList = qToPs.get(q);
        psList.add(psVar[i]);
      }
      ++i;
    }

    // objective function
    IntVar qCovered = m.intVar("qCovered", 0, 100000);
    IntVar[] qVar = new IntVar[querySet.size()];
    i = 0;
    for (String q : querySet) {
      qVar[i] = m.intVar(q, 0, 1);
      qMap.put(q, qVar[i]);
      ++i;
    }
    m.sum(qVar, "=", qCovered).post();

    // constraint 1: no more than k sets
    IntVar numPs = m.intVar("numPs", 0, partitionSpaces.size());
    m.sum(psVar, "=", numPs).post();
    m.arithm(numPs, "<=", k).post();

    // constraint 2: a query needs at least one ps chosen
    for (String q : qToPs.keySet()) {
      IntVar qVar1 = qMap.get(q);
      List<IntVar> psVarList = qToPs.get(q);
      IntVar[] psVars = psVarList.toArray(new IntVar[psVarList.size()]);

      IntVar qSum = m.intVar(q + "_sum", 0, 100000);
      m.sum(psVars, "=", qSum).post();
      m.arithm(qSum, ">=", qVar1).post();
    }

    m.setObjective(Model.MAXIMIZE, qCovered);
    m.getSolver().limitTime("30s");

    Solution solution = new Solution(m);
    while (m.getSolver().solve()) {
      m.getSolver().printStatistics();
      solution.record();
    }
    m.getSolver().printStatistics();

    for (i = 0; i < psVar.length; ++i) {
      System.out.println(String.format("ps_%d = %d", i, solution.getIntVal(psVar[i])));
    }
  }
}
