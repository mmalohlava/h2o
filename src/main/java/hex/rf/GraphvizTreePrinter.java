package hex.rf;

import hex.rf.Tree.ExclusionNode;
import hex.rf.Tree.LeafNode;
import hex.rf.Tree.SplitNode;

import java.io.*;
import java.text.MessageFormat;
import java.util.Arrays;

import water.AutoBuffer;
import water.ValueArray.Column;
import water.util.Utils;


public class GraphvizTreePrinter extends TreePrinter {
  private final Appendable _dest;

  public GraphvizTreePrinter(OutputStream dest, Column[] columns, int[] colMapping, String[]classNames) {
    this(new OutputStreamWriter(dest), columns, colMapping, classNames);
  }

  public GraphvizTreePrinter(Appendable dest, Column[] columns, int[] colMapping, String[]classNames) {
    super(columns, colMapping, classNames);
    _dest = dest;
  }

  public void printTree(Tree t) throws IOException {
    _dest.append("digraph {\n");
    t._tree.print(this);
    _dest.append("}");
    if( _dest instanceof Flushable ) ((Flushable) _dest).flush();
  }

  void printNode(LeafNode t) throws IOException {
    int obj = System.identityHashCode(t);
    _dest.append(String.format("%d [label=\"%s\\n%s\"];\n",
        obj, "Leaf Node",
        MessageFormat.format("Class {0}", Arrays.toString(t._classHisto) )));
  }


  @Override
  void printNode(SplitNode t) throws IOException {
    int obj = System.identityHashCode(t);

    _dest.append(String.format("%d [label=\"%s\\n%s\"];\n",
        obj, "Node",
        MessageFormat.format("data[{0}] <= {1} (gini)",
            _cols[t._column]._name, t._split)));

    t._l.print(this);
    t._r.print(this);

    int lhs = System.identityHashCode(t._l);
    int rhs = System.identityHashCode(t._r);
    _dest.append(String.format("%d -> %d;\n", obj, lhs));
    _dest.append(String.format("%d -> %d;\n", obj, rhs));
  }

  @Override
  void printNode(ExclusionNode t) throws IOException {
    int obj = System.identityHashCode(t);

    _dest.append(String.format("%d [label=\"%s\\n%s\"];\n",
        obj, "Node",
        MessageFormat.format("data[{0}] == {1} (gini)",
            _cols[t._column]._name, t._split)));

    t._l.print(this);
    t._r.print(this);

    int lhs = System.identityHashCode(t._l);
    int rhs = System.identityHashCode(t._r);
    _dest.append(String.format("%d -> %d;\n", obj, lhs));
    _dest.append(String.format("%d -> %d;\n", obj, rhs));
  }



  // Walk and print a serialized tree - we do not get a proper tree structure,
  // instead the deserializer walks us on the fly.
  public void walk_serialized_tree( AutoBuffer tbits ) {
    try {
      _dest.append("digraph {\n");
      new Tree.TreeVisitor<IOException>(tbits) {
        @Override protected Tree.TreeVisitor leaf(byte[] classHisto ) throws IOException {
          int tclass = Utils.maxIndex(classHisto);
          String x = _classNames != null && tclass < _classNames.length
            ? String.format("%d [label=\"%s\n%s\"];\n"      , _ts.position()-_classes-1, _classNames[tclass], Arrays.toString(classHisto))
            : String.format("%d [label=\"Majority Class %d\n%s\"];\n", _ts.position()-_classes-1, tclass, Arrays.toString(classHisto));
          _dest.append(x);
          return this;
        }
        @Override protected Tree.TreeVisitor pre (int col, float fcmp, int off0, int offl, int offr ) throws IOException {
          byte b = (byte) _ts.get1(off0);
          _dest.append(String.format("%d [label=\"%s %s %f\"];\n",
                                     off0, _cols[col]._name, ((b=='E')?"==":"<="), fcmp));
          _dest.append(String.format("%d -> %d;\n", off0, offl));
          _dest.append(String.format("%d -> %d;\n", off0, offr));
          return this;
        }
      }.visit();
      _dest.append("}");
      if( _dest instanceof Flushable ) ((Flushable) _dest).flush();
    } catch( IOException e ) { throw new Error(e); }
  }
}
