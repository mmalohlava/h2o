package hex.rf;

import hex.rf.Data.Row;
import hex.rf.Statistic.Split;

import java.util.Arrays;
import java.util.Stack;

import water.*;
import water.util.Log;
import water.util.Utils;


public class RefinedTree extends Tree {

  final Key _origTreeKey;
  final AutoBuffer _serialTree;
  final int _treeProducerIdx;
  final int _treeIdx; // tree idx in producer forest

  public RefinedTree(Job job, Key orgiTreeKey, AutoBuffer serialTree, int treeProducerIdx, int treeIdx, int treeId, long seed, Data data, int maxDepth, StatType stat, int numSplitFeatures,
      int exclusiveSplitLimit, Sampling sampler, int verbose, int nodesize) {
    super(job, data, maxDepth, stat, numSplitFeatures, seed, treeId, exclusiveSplitLimit, sampler, verbose, nodesize);
    _origTreeKey = orgiTreeKey; // tree of key being refined
    _serialTree = serialTree;
    _treeProducerIdx = treeProducerIdx;
    _treeIdx = treeIdx;
  }

  @Override public void compute2() {
    if (!_job.cancelled()) {
      System.err.println("Refining tree: " + _data_id);
      Timer timer = new Timer();
      _stats[0]   = new ThreadLocal<Statistic>();
      _stats[1]   = new ThreadLocal<Statistic>();
      Data      d = _sampler.sample(_data, _seed);
      // Recostructing tree in memory
      _tree = extractTree(_serialTree);
      // -------
//      System.err.println(dumpTree("Tree after load:\n"));
      // -------

      refine(d, null, _tree, 0);
      Log.info("RF refinement took: " + timer);
      // -------
//      System.err.println(dumpTree("Tree after refine:\n"));
      // -------

      updateRefinedTreeMatrix(_job.dest(), toKey(), _treeIdx, _treeProducerIdx);
    }
    tryComplete();
  }

  static void updateKey(final Key tKey, final AutoBuffer bs) {
    final byte[] bits = bs.buf();
    new water.Atomic() {
      @Override public Value atomic(Value val) {
        return new Value(tKey, bits);
      }
    }.invoke(tKey);
  }
  static void updateRefinedTreeMatrix(final Key rfModel, final Key tKey, final int treeIdx, final int treeProducerIdx) {
    final int nodeIdx = H2O.SELF.index();
    new TAtomic<RFModel>() {
      @Override public RFModel atomic(RFModel old) {
        if (old==null) return null;
        return RFModel.updateRTM(old, tKey, treeIdx, treeProducerIdx, nodeIdx);
      }
    }.invoke(rfModel);
  }

  void refine(Data d, SplitNode parent, INode tree, int depth) {
    if (tree.isLeaf()) {
      boolean isLeft = parent._l == tree;
      // try to refine the leaf
      Statistic stats = getStatistic(isLeft?0:1, d, _seed + (isLeft?LTSS_INIT:RTSS_INIT), _exclusiveSplitLimit);
      for (Row r : d) stats.addQ(r);
      stats.applyClassWeights();
      Split split = stats.split(d, false);
      if (! split.isLeafNode()) {
        INode node = new FJBuild(split, d, depth, _seed).compute();
//        Log.info("Leaf node refined!");
        if (isLeft) parent._l = node; else parent._r = node;
      } else {
        // do nothing  but just check if we obtain the same split class
        int oldPred = Utils.maxIndex(((LeafNode)tree)._classHisto);
        byte[] histo = d.histogram();
        int newPred = Utils.maxIndex(histo);
//        if (oldPred!=newPred) Log.warn("Leaf refinement stop at leaf but predict different class! " + oldPred+"!="+newPred);
      }
    } else { // It is a split node
      SplitNode sn = (SplitNode) tree;
      // split data into L/R parts and recall split on the L/R nodes
      Data[] lrData = new Data[2];
      d.split(sn, lrData);
      if (lrData[0].rows() > 0) refine(lrData[0], sn, sn._l, depth+1);
      if (lrData[1].rows() > 0) refine(lrData[1], sn, sn._r, depth+1);
    }
  }
  static INode extractTree(AutoBuffer sTree) {
    TreeExtractor te = new TreeExtractor(sTree);
    te.visit();
    return te.getRoot();
  }

  static class TreeExtractor extends TreeVisitor<RuntimeException> {
    Stack<INode> _nodes = new Stack<Tree.INode>();

    public TreeExtractor(AutoBuffer tbits) { super(tbits); }
    @Override protected TreeVisitor<RuntimeException> leaf(byte[] tclass) {
      _nodes.push(new LeafNode(Arrays.copyOf(tclass, tclass.length), 0));
      return this;
    }
    protected Tree.TreeVisitor<RuntimeException> post(int col, float fcmp) {
      INode right = _nodes.pop();
      INode left  = _nodes.pop();
      SplitNode splitNode = new SplitNode(col, Integer.MIN_VALUE, null, fcmp);
      splitNode._r = right;
      splitNode._l = left;
      _nodes.push(splitNode);
      return this;
    };
    INode getRoot() {
      assert _nodes.size() == 1;
      return _nodes.peek();
    }
  }
}

