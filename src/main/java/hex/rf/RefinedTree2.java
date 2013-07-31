package hex.rf;

import hex.rf.Statistic.Split;
import water.*;

public class RefinedTree2 extends RefinedTree {

  public RefinedTree2(TRunnable<RefinedTree> afterAction, Job job, Key origTreeKey, AutoBuffer serialTree,
      int treeProducerIdx, int treeIdx, int treeId, long seed, Data data, int maxDepth, StatType stat,
      int numSplitFeatures, int exclusiveSplitLimit, Sampling sampler, int verbose, int nodesize) {
    super(afterAction, job, origTreeKey, serialTree, treeProducerIdx, treeIdx, treeId, seed, data, maxDepth, stat,
        numSplitFeatures, exclusiveSplitLimit, sampler, verbose, nodesize);
  }

  @Override INode refineLeafSplit(Split split, Data data, int depth, LeafNode leaf) {
    return new LeafNode(data.histogram(), data.rows());
  }
  @Override INode refineNonLeafSplit(Split split, Data data, int depth, LeafNode leaf) {
    return new LeafNode(data.histogram(), data.rows());
  }

}
