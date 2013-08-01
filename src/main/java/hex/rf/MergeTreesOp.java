package hex.rf;

import java.util.UUID;

import hex.rf.Tree.*;
import water.*;
import water.H2O.H2OCountedCompleter;

public class MergeTreesOp extends H2OCountedCompleter {
  final Key _masterTree;
  final Key _refinedTree;
  final Key _rfModel;
  public MergeTreesOp(Key rfModel, Key masterTree, Key refinedTree) {
    _rfModel = rfModel; _masterTree = masterTree; _refinedTree = refinedTree;
  }
  @Override public void compute2() {
    byte[] mtb = DKV.get(_masterTree).memOrLoad();
    byte[] rtb = DKV.get(_refinedTree).memOrLoad();
    assert Tree.treeId(mtb) == Tree.treeId(rtb);
    assert Tree.seed(mtb)   == Tree.seed(rtb);
    assert Tree.classes(mtb) == Tree.classes(rtb);
    assert Tree.round(mtb) == Tree.round(rtb);
    INode mt = RefinedTree.extractTree(new AutoBuffer(mtb));
    INode rt = RefinedTree.extractTree(new AutoBuffer(rtb));
    merge(mt, null, rt, null);
    // FIXME now we should decide if merge tree is better or not
    byte[] mtab = RefinedTree.serialize(Tree.treeId(mtb), Tree.seed(mtb), Tree.classes(mtb), Tree.round(mtb), (byte)-1, mt).buf();
    updateTKey(_masterTree, mtab);
//    appendTree(_rfModel, mtab);
    tryComplete();
  }

  static void merge(INode t1, INode p1, INode t2, INode p2) {
    // Simple descent over split nodes
    if (compare(t1, t2) == 0) {
      merge(asSplit(t1)._l, t1, asSplit(t2)._l, t2); // merge left sub-tree
      merge(asSplit(t1)._r, t1, asSplit(t2)._r, t2); // merge right sub-tree
    } else if (isLeaf(t1) && isLeaf(t2)) {
      // merge histograms in ratio of affected rows t1:t2 (~simluate importance of the leaf)
      byte[] mh = mergeHisto(asLeaf(t1), asLeaf(t2));
      LeafNode ln = new LeafNode(mh, asLeaf(t1)._rows + asLeaf(t2)._rows);
      if (isLeft(t1, asSplit(p1))) asSplit(p1)._l = ln; else asSplit(p1)._r = ln;
    } else if (isLeaf(t1) && isSplit(t2)) {
      if (isLeft(t1, asSplit(p1))) asSplit(p1)._l = t2; else asSplit(p1)._r = t2;
    }
  }

  static final boolean isLeft (INode n, SplitNode s) { return s._l == n; }
  static final boolean isSplit(INode n) { return n instanceof SplitNode; }
  static final boolean isLeaf (INode n) { return n instanceof LeafNode; }
  static final LeafNode  asLeaf (INode n) { return (LeafNode) n; }
  static final SplitNode asSplit(INode n) { return (SplitNode) n; }

  static INode mergeNodes(INode n, LeafNode l) {
    if (n.isLeaf()) return mergeNodes(asLeaf(n), l);
    SplitNode sn = asSplit(n);
    INode left = mergeNodes(sn._l, l);
    sn._l = left;
    INode right = mergeNodes(sn._r, l);
    sn._r = right;
    return n;
  }

  static INode mergeNodes(LeafNode l1, LeafNode l2) {
    byte[] mhisto = mergeHisto(l1, l2);
    return new LeafNode(mhisto, l1._rows+l2._rows);
  }
  static byte[] mergeHisto(LeafNode l1, LeafNode l2) {
    byte[] h1 = l1._classHisto; int ar1 = l1._rows;
    byte[] h2 = l2._classHisto; int ar2 = l2._rows;
    assert h1.length == h2.length;
    byte[] h  = new byte[h1.length];
    float k1 = ar1/(float)(ar1+ar2);
    float k2 = ar2/(float)(ar1+ar2);
    float sum = 0;
    // k1 = 1; k2 =1; // do not apply weights
    for (int i=0;i<h1.length;i++) sum += k1*h1[i] + k2*h2[i];
    for (int i=0;i<h1.length;i++) h[i] = (byte) ((k1*h1[i] + k2*h2[i])*100/sum);
    return h;
  }

  static final double EPSILON = 0.01;
  public static int compare(INode t1, INode t2) {
    if (isSplit(t1) && isSplit(t2) &&
      asSplit(t1)._column  == asSplit(t2)._column &&
      Math.abs(asSplit(t1)._originalSplit - asSplit(t2)._originalSplit) < EPSILON) {
      return 0;
    }
    return -1;
  }

  static void appendTree(Key rfModel, final byte[] value) {
    Key tKey = Key.make(UUID.randomUUID().toString(),(byte)1,Key.DFJ_INTERNAL_USER, H2O.SELF);
    DKV.put(tKey,new Value(tKey, value));
    Tree.appendKey(rfModel, tKey,-1);
  }

  static void updateTKey(final Key k, final byte[] value) {
    new Atomic() {
      @Override public Value atomic(Value val) {
        return new Value(k, value);
      }
    }.invoke(k);
  }

}