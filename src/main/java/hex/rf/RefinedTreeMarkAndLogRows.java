package hex.rf;

import hex.rf.Data.Row;
import hex.rf.Statistic.Split;

import java.util.*;

import water.*;
import water.util.Utils;

/**
 * This tree identify only parts of tree which need refinement.
 * It collects the data which are potential reason for refinement
 * of tree and save data in form of ValueArray
 *
 * @author michal
 *
 */
public class RefinedTreeMarkAndLogRows extends RefinedTree {

  private final Map<Integer, Integer> _chunks = new HashMap<Integer, Integer>(); // indexes of home chunks

  public RefinedTreeMarkAndLogRows(TRunnable<RefinedTree> afterAction, Job job, byte round, Key origTreeKey, AutoBuffer serialTree,
      byte treeProducerIdx, int treeIdx, int treeId, long seed, Data data, int maxDepth, StatType stat,
      int numSplitFeatures, int exclusiveSplitLimit, Sampling sampler, int verbose, int nodesize) {
    super(PRINT_CHUNKS, job, round, origTreeKey, serialTree, treeProducerIdx, treeIdx, treeId, seed, data, maxDepth, stat,
        numSplitFeatures, exclusiveSplitLimit, sampler, verbose, nodesize);
  }

  @Override INode refineLeafSplit(Split split, Data data, int depth, LeafNode leaf) {
    // Save data only if histograms are different
    if (!histoDiffer(data, leaf)) {
      identifyChunks(data);
    }
    // do not modify tree
    return leaf;
  }
  @Override INode refineNonLeafSplit(Split split, Data data, int depth, LeafNode leaf) {
    if (!histoDiffer(data, leaf)) {
      identifyChunks(data);
    }
    // do not modify tree
    return leaf;
  }

  final void identifyChunks(Data data) {
    Iterator<Row> it = data.iterator();
    while (it.hasNext()) {
      Row row = it.next();
      int idx = row._index / data._dapt._ary.rpc(0); // this is not exactly right since chunks can has different sizes...
      if (!_chunks.containsKey(idx))
        _chunks.put(idx, 1);
      else
        _chunks.put(idx, 1+_chunks.get(idx));
    }
  }

  final boolean histoDiffer(Data data, LeafNode leaf) {
    byte[] oldHisto = leaf.histogram();
    byte[] newHisto = data.histogram();
    int oMajClass = Utils.maxIndex(oldHisto);
    int nMajClass = Utils.maxIndex(newHisto);
    if (oMajClass != nMajClass) {
      // we are different here
      //Log.info("Majority class differs: " + Arrays.toString(oldHisto) + " (" + leaf._rows +") v. " + Arrays.toString(newHisto) + "("+ data.rows() + ")");
      return true;
    }
    return false;
  }

  public static final TRunnable<RefinedTree> PRINT_CHUNKS = new TRunnable<RefinedTree>() {

    @Override public void run(RefinedTree t) {
      RefinedTreeMarkAndLogRows r = (RefinedTreeMarkAndLogRows)t;
      System.err.println("Node " + r._producerIdx + " needs " + Arrays.toString(r._chunks.entrySet().toArray()) + " home chunks&=&rows from " + H2O.SELF.index() );
    }

  };
}
