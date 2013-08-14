package hex.rf;

import hex.rf.Data.Row;
import hex.rf.Statistic.Split;

import java.util.Arrays;
import java.util.Stack;

import jsr166y.CountedCompleter;

import water.*;
import water.util.*;
import water.util.Log.Tag.Sys;
import static hex.rf.MergeTreesOp.asLeaf;
import static hex.rf.MergeTreesOp.mergeNodes;

public class RefinedTree extends Tree {

  final Key _origTreeKey;
  final AutoBuffer _serialTree;
  final int _treeIdx; // tree idx in producer forest
  final TRunnable _afterAction;

  public RefinedTree(TRunnable<RefinedTree> afterAction, Job job, byte round, Key origTreeKey, AutoBuffer serialTree, byte treeProducerIdx, int treeIdx, int treeId, long seed, Data data, int maxDepth, StatType stat, int numSplitFeatures,
      int exclusiveSplitLimit, Sampling sampler, int verbose, int nodesize) {
    super(job, data, round, treeProducerIdx, maxDepth, stat, numSplitFeatures, seed, treeId, exclusiveSplitLimit, sampler, verbose, nodesize);
    assert treeIdx == treeId : "Refined treeId and treeId do not match!";
    _afterAction = afterAction;
    _origTreeKey = origTreeKey; // tree of key being refined
    _serialTree = serialTree;
    _treeIdx = treeIdx;
  }

  @Override public void compute2() {
    if (!_job.cancelled()) {
      Log.info("Refining tree: " + _treeId + " from node: " + _producerIdx + " on node: " + H2O.SELF.index() + " round: " + _round);
      Timer timer = new Timer();
      _stats[0]   = new ThreadLocal<Statistic>();
      _stats[1]   = new ThreadLocal<Statistic>();
      Data      d = _sampler.sample(_data, _seed);
      // Recostructing tree in memory
      _tree = extractTree(_serialTree);
//      System.err.println(dumpTree("Tree after load:\n"));
      refine(d, null, _tree, 0);
      Log.info("Tree id: " +_treeId + " from: " + _producerIdx + " refinement took: " + timer);
//      System.err.println(dumpTree("Tree after refine:\n"));

      if (_afterAction!=null)
        _afterAction.run(this);
    }
    tryComplete();
  }

  void refine(Data d, SplitNode parent, INode tree, int depth) {
    if (tree.isLeaf() && parent!=null) { // refine only non-trivial tree
      boolean isLeft = parent._l == tree;
      assert isLeft || parent._r == tree;
      // try to refine the leaf
      Statistic stats = getStatistic(isLeft?0:1, d, _seed + (isLeft?LTSS_INIT:RTSS_INIT), _exclusiveSplitLimit);
      for (Row r : d) stats.addQ(r);
      stats.applyClassWeights();
      Split split = stats.split(d, false); // FIXME - here we need to know the original data in leaf
      INode newNode;
      if (! split.isLeafNode()) {
    	  newNode = refineNonLeafSplit(split, d, depth, asLeaf(tree));
      } else {
        //if (split.isImpossible()) Log.info("Refine tree: hit imposible split:");
        newNode = refineLeafSplit(split, d, depth, asLeaf(tree));
      }
      if (isLeft) parent._l = newNode; else parent._r = newNode;
    } else if (!tree.isLeaf()) { // It is a split node
      SplitNode sn = (SplitNode) tree;
      // split data into L/R parts and recall split on the L/R nodes
      Data[] lrData = new Data[2];
      d.split(sn, lrData);
      if (lrData[0].rows() > 0) refine(lrData[0], sn, sn._l, depth+1);
      if (lrData[1].rows() > 0) refine(lrData[1], sn, sn._r, depth+1);
    }
  }

  // -- Possible strategies:
  // 1. replace node by a refined subtree
  // 2. replace node by a refined subtree but merge histogram
  // ---
  // replace the leaf by a new subtree, but merge its leaves with original leaf
  // to preserve leaf histogram
  INode refineNonLeafSplit(Split split, Data data, int depth, LeafNode leaf) {
//    Log.info("Split! Observatios: " + data.rows() + "(original: " + leaf._rows + ")");
    INode subtree = new FJBuild(split, data, depth, _seed).compute();
    return mergeNodes(subtree, leaf);
//    return leaf;
  }
  // -- Possible strategies
  // 1. replace old leaf by a new leaf
  // 2. merge old and new leaf
  // --
  // do nothing  but just check if we obtain the same split class
  INode refineLeafSplit(Split split, Data data, int depth, LeafNode leaf) {
    byte[] histo = data.histogram();
    LeafNode expLeaf = new LeafNode(histo, data.rows()); // expected leaf
    byte[] newHisto = MergeTreesOp.mergeHisto(leaf, expLeaf); // merge histograms
    return new LeafNode(newHisto, leaf._rows+expLeaf._rows);
//    return leaf;
  }
  static INode extractTree(AutoBuffer sTree) {
    TreeExtractor te = new TreeExtractor(sTree);
    te.visit();
    return te.getRoot();
  }

  /** Tree visitor extracting tree from its serialized format. */
  static class TreeExtractor extends TreeVisitor<RuntimeException> {
    Stack<INode> _nodes = new Stack<Tree.INode>();

    public TreeExtractor(AutoBuffer tbits) { super(tbits); }
    @Override protected TreeVisitor<RuntimeException> leaf(int aRows, byte[] tclass) {
      _nodes.push(new LeafNode(Arrays.copyOf(tclass, tclass.length), aRows));
      return this;
    }
    @Override protected Tree.TreeVisitor<RuntimeException> post(int col, float fcmp, byte producerIdx ) {
      INode right = _nodes.pop();
      INode left  = _nodes.pop();
      SplitNode splitNode = new SplitNode(col, Integer.MIN_VALUE, null, fcmp, producerIdx);
      splitNode._r = right;
      splitNode._l = left;
      _nodes.push(splitNode);
      return this;
    }
    INode getRoot() {
      assert _nodes.size() == 1;
      return _nodes.peek();
    }
  }

  /** Simple prefix tree merge. */
  public void mergeWith(INode tree) {
    INode thisTree = _tree;
    MergeTreesOp.merge(thisTree, null, tree, null);
  }

  public static enum Strategy {
    APPEND,
    MERGE,
    MERGE_AND_APPEND,
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

  public static interface TRunnable<T> {
      void run(T t);
  }

  public static final TRunnable<RefinedTree> UPDATE_KEY_ACTION = new TRunnable<RefinedTree>() {
    @Override public void run(RefinedTree t) {
      updateKey(t._origTreeKey, t.serialize());
    }
  };

  public static final TRunnable<RefinedTree> UPDATE_RTM = new TRunnable<RefinedTree>() {
    @Override public void run(RefinedTree t) {
      updateRefinedTreeMatrix(t._job.dest(), t.toKey(), t._treeIdx, t._producerIdx);
    }
  };
}

