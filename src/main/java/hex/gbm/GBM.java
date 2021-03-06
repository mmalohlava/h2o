package hex.gbm;

import java.util.ArrayList;
import java.util.Arrays;
import water.*;
import water.H2O.H2OCountedCompleter;
import water.fvec.*;
import water.util.Log;

// Gradient Boosted Trees
public class GBM extends Job {
  public static final String KEY_PREFIX = "__GBMModel_";

  public static final Key makeKey() { return Key.make(KEY_PREFIX + Key.make());  }
  private GBM(Key dest, Frame fr) { super("GBM "+fr, dest); }
  // Called from a non-FJ thread; makea a GBM and hands it over to FJ threads
  public static GBM start(Key dest, final Frame fr, final int maxDepth) {
    final GBM job = new GBM(dest, fr);
    H2O.submitTask(job.start(new H2OCountedCompleter() {
        @Override public void compute2() { job.run(fr,maxDepth); tryComplete(); }
      })); 
    return job;
  }

  // ==========================================================================

  // Compute a GBM tree.  

  // Start by splitting all the data according to some criteria (minimize
  // variance at the leaves).  Record on each row which split it goes to, and
  // assign a split number to it (for next pass).  On *this* pass, use the
  // split-number to build a per-split histogram, with a per-histogram-bucket
  // variance.

  // Compute a single GBM tree from the Frame.  Last column is the response
  // variable.  Depth is capped at maxDepth.
  private void run(Frame fr, int maxDepth) {
    final int ncols = fr._vecs.length-1; // Last column is the response column
    Vec vs[] = fr._vecs;

    // Make a new Vec to hold the split-number for each row (initially all zero).
    Vec vsplit = Vec.makeZero(vs[0]);
    // Make a new Vec to hold the prediction value for each row
    Vec vpred  = Vec.makeZero(vs[0]);

    // Initially setup as-if an empty-split had just happened
    Histogram hists[] = new Histogram[ncols];
    for( int j=0; j<ncols; j++ )
      hists[j] = new Histogram(fr._names[j],vs[j].length(),vs[j].min(),vs[j].max(),vs[j]._isInt);
    Tree root = new Tree(null,hists,0);
    ArrayList<Tree> wood = new ArrayList<Tree>();
    wood.add(root);
    int treeMin = 0, treeMax = 1; // Define a "working set" of leaf splits

    // One Big Loop till the tree is of proper depth.
    for( int depth=0; depth<maxDepth; depth++ ) {

      // Report the average prediction error
      double error = new CalcError().invoke(fr._vecs[ncols],vpred)._sum;
      double errAvg = error/fr._vecs[0].length();
      Log.unwrap(System.out,"============================================================== ");
      Log.unwrap(System.out,"Average prediction error for tree of depth "+depth+" is "+errAvg);

      // Build a histogram with a pass over the data.
      // Should be an MRTask2.
      for( int k=0; k<fr._vecs[0].length(); k++ ) {
        int split = (int)vsplit.at8(k);   // Split for this row
        if( split == -1 ) continue;  // Row does not need to split again (generally perfect prediction already)
        double y = fr._vecs[ncols].at(k); // Response variable for row
        Tree t = wood.get(split);         // This row is being split in Tree t
        for( int j=0; j<ncols; j++ )      // For all columns
          if( t._hs[j] != null ) // Some columns are ignored, since already split to death
            t._hs[j].incr(fr._vecs[j].at(k),y);
      }

      // Compute mean-square-error, per-column.  
      // Requires mean from 1st pass, so requires a 2nd pass.
      //for( int k=0; k<fr._vecs[0].length(); k++ ) {
      //  int split = (int)vsplit.at8(k);        // Split for this row
      //  if( split == -1 ) continue;  // Row does not need to split again (generally perfect prediction already)
      //  double y = fr._vecs[ncols].at(k);      // Response variable for row
      //  Tree t = wood.get(split);    // This row is being split in Tree t
      //  for( int j=0; j<ncols; j++ ) // For all columns
      //    if( t._hs[j] != null ) // Some columns are ignored, since already split to death
      //      t._hs[j].incr2(fr._vecs[j].at(k),y);
      //}

      // Build up the next-generation tree splits from the current histograms.
      // Nearly all leaves will split one more level.  This loop nest is
      // O( #active_splits * #bins * #ncols )
      int nsplit=treeMax;
      for( int w=treeMin; w<treeMax; w++ ) {
        Tree t = wood.get(w);         // Tree being split

        // Adjust for observed min/max in the histograms, as some of the bins
        // might already be zero from a prior split (i.e., the maximal element
        // in column 7 went to the "other split"), leaving behind a smaller new
        // maximal element.
        for( int j=0; j<ncols; j++ ) { // For every column in the new split
          Histogram h = t._hs[j];      // Old histogram of column
          if( h == null ) continue;    // Column was not being tracked?
          int n = 0;
          while( h._bins[n]==0 ) n++;  // First non-empty bin
          int l = h._bins.length-1;    // Last bin
          if( n > 0 ) h._mins[0] = h._mins[n];
          int x = l;  
          while( h._bins[x]==0 ) x--;  // Last non-empty bin
          if( x < l ) h._maxs[l] = h._maxs[x];
        }
        
        // Compute best split
        int col = t.bestSplit();      // Best split-point for this tree
        Log.unwrap(System.out,t.toString());

        // Split the best split.  This is a BIN-way split; each bin gets it's
        // own subtree.
        Histogram splitH = t._hs[col];// Histogram of the column being split
        int l = splitH._bins.length;  // Number of split choices
        assert l > 1;                 // 
        for( int i=0; i<l; i++ ) {    // For all split-points
          if( splitH._bins[i] <= 1 ) continue; // Zero or 1 elements
          if( splitH.var(i) == 0.0 ) continue; // No point in splitting a perfect prediction

          // Build a next-gen split point from the splitting bin
          Histogram nhists[] = new Histogram[ncols]; // A new histogram set
          for( int j=0; j<ncols; j++ ) { // For every column in the new split
            Histogram h = t._hs[j];      // Old histogram of column
            if( h == null ) continue;    // Column was not being tracked?
            // min & max come from the original column data, since splitting on
            // an unrelated column will not change the j'th columns min/max.
            double min = h._mins[0], max = h._maxs[h._maxs.length-1];
            // Tighter bounds on the column getting split: exactly each new
            // Histogram's bound are the bins' min & max.
            if( col==j ) { min=h._mins[i]; max=h._maxs[i]; }
            if( min == max ) continue; // This column will not split again
            nhists[j] = new Histogram(fr._names[j],splitH._bins[i],min,max,vs[j]._isInt);
          }
          // Add a new (unsplit) Tree
          wood.add(t._ts[i]=new Tree(t,nhists,nsplit++));
        }
      }
      // "Score" each row against the new splits.  Assign a new split.
      // Should be an MRTask2.
      for( int k=0; k<fr._vecs[0].length(); k++ ) {
        double y = fr._vecs[ncols].at(k);  // Response variable for row
        int oldSplit = (int)vsplit.at8(k); // Get the tree/split number
        if( oldSplit == -1 ) continue;     // row already predicts perfectly
        Tree t = wood.get(oldSplit);       // This row is being split in Tree t
        double d = fr._vecs[t._col].at(k); // Value to split on
        int bin = t._hs[t._col].bin(d);    // Bin in the histogram in the tree
        double pred = t._hs[t._col].mean(bin);// Current prediction
        Tree tsplit = t._ts[bin];          // Tree split
        int newSplit = tsplit==null ? -1 : tsplit._split; // New split# or -1 for "row is done"
        //StringBuilder sb = new StringBuilder("{");
        //for( int j=0; j<ncols; j++ )
        //  sb.append(fr._vecs[j].at(k)).append(",");
        //sb.append("}=").append(y).append(", pred=").append(pred).append(", split=").append(newSplit);
        //Log.unwrap(System.out,sb.toString());
        vsplit.set8(k,newSplit); // Save the new split# for this row
        vpred .set8(k,pred);     // Save the new prediction also
      }
      for( int i=0; i<vsplit.nChunks(); i++ ) {
        vsplit.elem2BV(i).close(i,null); // "close" the written vsplit vec
        vpred .elem2BV(i).close(i,null); // "close" the written vsplit vec
      }

      treeMin = treeMax;        // Move the "working set" of splits forward
      treeMax = nsplit;
    }

    // One more pass for prediction error
    double error=0;
    for( int k=0; k<fr._vecs[0].length(); k++ ) {
      double y = fr._vecs[ncols].at(k);      // Response variable for row
      double z = y-vpred.at(k);              // Error for this prediction
      error += z*z;                          // Cumlative error
    }
    double errAvg = error/fr._vecs[0].length();
    Log.unwrap(System.out,"Average prediction error for tree of depth "+maxDepth+" is "+errAvg);

    // Remove temp vectors
    UKV.remove(vsplit._key);
    UKV.remove(vpred ._key);
  }

  // --------------------------------------------------------------------------
  // Compute sum-squared-error
  private static class CalcError extends MRTask2<CalcError> {
    double _sum;
    @Override public void map( Chunk ys, Chunk preds ) {
      for( int i=0; i<ys._len; i++ ) {
        assert !ys.isNA0(i);
        double y    = ys   .at0(i);
        double pred = preds.at0(i);
        _sum += (y-pred)*(y-pred);
      }
    }
    @Override public void reduce( CalcError t ) { _sum += t._sum; }
  }

  // --------------------------------------------------------------------------
  // A tree of splits.  Each node describes how to split the datarows into
  // smaller subsets... or describes a leaf with a specific regression.
  private static class Tree extends Iced {
    final Tree _parent;         // Parent tree
    final Histogram _hs[];      // Histograms per column
    final int _split;           // Split# for this tree
    Tree _ts[];                 // Child trees (maybe more than 2)
    int _col;                   // Column we split over
    Tree( Tree parent, Histogram hs[], int split ) { _parent=parent; _hs = hs; _split=split; }
    // Find the column with the best split (lowest score).
    // Also setup leaf Trees for when we split this Tree
    int bestSplit() {
      assert _ts==null;
      double bs = Double.MAX_VALUE; // Best score
      int idx = -1;             // Column to split on
      for( int i=0; i<_hs.length; i++ ) {
        if( _hs[i]==null || _hs[i]._bins.length == 1 ) continue;
        double s = _hs[i].scoreVar();
        if( s < bs ) { bs = s; idx = i; }
      }
      // Split on column 'idx' with score 'bs'
      _ts = new Tree[_hs[idx]._bins.length];
      return (_col=idx);
    }

    @Override public String toString() {
      final String colPad="  ";
      final int cntW=4, mmmW=4, varW=4;
      final int colW=cntW+1+mmmW+1+mmmW+1+mmmW+1+varW;
      StringBuilder sb = new StringBuilder();
      sb.append("Split# ").append(_split).append(", ");
      printLine(sb).append("\n");
      final int ncols = _hs.length;
      for( int j=0; j<ncols; j++ )
        if( _hs[j] != null )
          p(sb,_hs[j]._name+String.format(", %5.2f",_hs[j].scoreVar()),colW).append(colPad);
      sb.append('\n');
      for( int j=0; j<ncols; j++ ) {
        if( _hs[j] == null ) continue;
        p(sb,"cnt" ,cntW).append('/');
        p(sb,"min" ,mmmW).append('/');
        p(sb,"max" ,mmmW).append('/');
        p(sb,"mean",mmmW).append('/');
        p(sb,"var" ,varW).append(colPad);
      }
      sb.append('\n');
      for( int i=0; i<Histogram.BINS; i++ ) {
        for( int j=0; j<ncols; j++ ) {
          if( _hs[j] == null ) continue;
          if( i < _hs[j]._bins.length ) {
            p(sb,Long.toString(_hs[j]._bins[i]),cntW).append('/');
            p(sb,              _hs[j]._mins[i] ,mmmW).append('/');
            p(sb,              _hs[j]._maxs[i] ,mmmW).append('/');
            p(sb,              _hs[j]. mean(i) ,mmmW).append('/');
            p(sb,              _hs[j]. var (i) ,varW).append(colPad);
            //p(sb,              _hs[j]. mse (i) ,varW).append(colPad);
          } else {
            p(sb,"",colW).append(colPad);
          }
        }
        sb.append('\n');
      }
      sb.append("Split# ").append(_split);
      if( _ts != null ) sb.append(", split on column "+_col+", "+_hs[_col]._name);
      return sb.toString();
    }
    static private StringBuilder p(StringBuilder sb, String s, int w) {
      return sb.append(Log.fixedLength(s,w));
    }
    static private StringBuilder p(StringBuilder sb, double d, int w) {
      String s = Double.isNaN(d) ? "NaN" :
        ((d==Double.MAX_VALUE || d==-Double.MAX_VALUE) ? " -" : 
         Double.toString(d));
      if( s.length() <= w ) return p(sb,s,w);
      s = String.format("%4.1f",d);
      if( s.length() > w )
        s = String.format("%4.0f",d);
      return sb.append(s);
    }

    StringBuilder printLine(StringBuilder sb ) {
      if( _parent==null ) return sb.append("root");
      Tree ts[] = _parent._ts;
      Histogram h = _parent._hs[_parent._col];
      for( int i=0; i<ts.length; i++ )
        if( ts[i]==this )
          return _parent.printLine(sb.append("[").append(h._mins[i]).append(" <= ").append(h._name).
                                   append(" <= ").append(h._maxs[i]).append("] from "));
      throw H2O.fail();
    }


  }

  private static double[] removeCol( double[] A, int i ) {
    double B[] = Arrays.copyOf(A,A.length-1);
    System.arraycopy(A,i+1,B,i,B.length-i);
    return B;
  }
  private static <T> T[] removeCol( T[] A, int i ) {
    T B[] = Arrays.copyOf(A,A.length-1);
    System.arraycopy(A,i+1,B,i,B.length-i);
    return B;
  }

}
