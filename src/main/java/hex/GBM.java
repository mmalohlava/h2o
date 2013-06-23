package hex;

import water.*;
import water.H2O.H2OCountedCompleter;
import water.fvec.*;
import water.util.Log;
import java.util.Formatter;

public class GBM extends Job {
  public static final String KEY_PREFIX = "__GBMModel_";

  public static final Key makeKey() { return Key.make(KEY_PREFIX + Key.make());  }
  private GBM(Key dest, Frame fr) { super("GBM "+fr, dest); }
  // Called from a non-FJ thread; makea a GBM and hands it over to FJ threads
  public static GBM start(Key dest, final Frame fr) {
    final GBM job = new GBM(dest, fr);
    H2O.submitTask(job.start(new H2OCountedCompleter() {
        @Override public void compute2() { job.run(fr); tryComplete(); }
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

  int _splitLevel;              // Tree split level.
  
  // Number of active splits at this level
  int _numSplits;

  // A histogram; one per split and each one has Histogram.BINS bins.
  Histogram _hists[];

  // Compute a single GBM tree
  private void run(Frame fr) {
    // Initially setup as-if an empty-split had just happened
    _numSplits = 1;
    final int ncols = fr._vecs.length-1; // Last column is the response column
    double mins[] = new double[ncols], maxs[] = new double[ncols];
    for( int i=0; i<ncols; i++ ) {
      mins[i] = fr._vecs[i].min();
      maxs[i] = fr._vecs[i].max();
    }
    Histogram hist = new Histogram(fr._names,fr._vecs[0].length(), mins, maxs);
    Log.unwrap(System.out,hist.toString()+"\n\n");

    double[] ds = new double[ncols+1];
    for( int i=0; i<ncols; i++ )
      ds[i] = fr._vecs[i].at(0);
    hist.incr(ds);
    Log.unwrap(System.out,hist.toString()+"\n\n");
    
    //while( true ) {
    //
    //  // Build an array of histograms, one histogram per split.
    //  // The histogram array is "ragged" - we use smaller arrays
    //  // to avoid having too few elements per bin.
    //  _hists = new Histogram[_numRowsPerSplit];
    //  for( int i=0; i<_numRowsPerSplit; i++ ) 
    //    _hists[i] = new Histogram(_numRowsPerSplit[i],x);
    //
    //  
    //}
  }

  // A Histogram over a particular Split.  The histogram runs from min to max
  // per each column (i.e., we actually make #cols histograms in parallel), and
  // is given the number of elements that will land in some bin (for small
  // enough elements, we make fewer bins).  Each column's range is independent
  // and recomputed at each split/histogram
  private static class Histogram extends Iced {
    public static final int BINS=4;
    transient final String[] _names; // Column names
    public final double[]   _steps;  // Linear interpolation step per bin
    public final long  [][] _bins;   // Bins by column, then bin
    public final double[][] _Ms;     // Rolling mean, per-column-per-bin
    public final double[][] _Ss;     // Rolling var , per-column-per-bin
    public final double[][] _mins, _maxs; // Per-column-per-bin min/max
    
    public Histogram( String[] names, long nelems, double[] mins, double[] maxs ) {
      assert nelems >= 0;
      _names = names;
      int nbins = Math.max((int)Math.min(BINS,nelems),1);
      int ncols = mins.length;
      _steps= new double[ncols];
      _bins = new long  [ncols][nbins]; // Counts per bin
      _Ms   = new double[ncols][nbins]; // Rolling bin mean
      _Ss   = new double[ncols][nbins]; // Rolling bin Variance*(cnt-1)
      _mins = new double[ncols][nbins]; // Rolling min per-bin
      _maxs = new double[ncols][nbins]; // Rolling max per-bin
      for( int i=0; i<ncols; i++ ) {    
        assert maxs[i] >= mins[i];
        _steps[i] = (maxs[i]-mins[i])/nbins; // Step size for linear interpolation
        for( int j=0; j<nbins; j++ ) { // Set bad bounds for min/max
          _mins[i][j] = Double.MAX_VALUE;
          _maxs[i][j] = Double.MIN_VALUE;
        }
        _mins[i][      0] = mins[i]; // Know better bounds for whole column min/max
        _maxs[i][nbins-1] = maxs[i];
      }
    }

    // Add 1 count to bin specified by double, for all doubles in a row.
    // Simple linear interpolation to specify bin.  Last column is response
    // variable; also add to the variance per-bin using the recursive strategy.
    //    http://www.johndcook.com/standard_deviation.html
    void incr( double[] ds ) {
      assert _bins.length == ds.length-1;
      double y = ds[ds.length-1];
      int nbins = _bins[0].length;
      for( int i=0; i<ds.length-1; i++ ) {
        double d = ds[i];
        int idx = _steps[i] <= 0.0 ? 0 : (int)((d-_mins[i][0])/_steps[i]);
        int idx2 = Math.max(Math.min(idx,nbins),0); // saturate at bounds
        _bins[i][idx2]++;
        // Track actual lower/upper bound per-bin
        if( d < _mins[i][idx2] ) _mins[i][idx2] = d;
        if( d > _maxs[i][idx2] ) _maxs[i][idx2] = d;
        // Recursive mean & variance
        //    http://www.johndcook.com/standard_deviation.html
        long k = _bins[i][idx2];
        double oldM = _Ms[i][idx2], newM = oldM + (d-oldM)/k;
        double oldS = _Ss[i][idx2], newS = oldS + (d-oldM)*(d-newM);
        _Ms[i][idx2] = newM;
        _Ss[i][idx2] = newS;
      }
    }
    double mean( int col, int bin ) { return _Ms[col][bin]; }
    double var ( int col, int bin ) { 
      return _bins[col][bin] > 1 ? _Ss[col][bin]/(_bins[col][bin]-1) : 0; 
    }

    // Pretty-print a histogram
    @Override public String toString() {
      final String colPad="  ";
      final int cntW=4, mmmW=4, varW=4;
      final int colW=cntW+1+mmmW+1+mmmW+1+mmmW+1+varW;
      StringBuilder sb = new StringBuilder();
      for( int j=0; j<_bins.length; j++ )
        p(sb,_names[j],colW).append(colPad);
      sb.append('\n');
      for( int j=0; j<_bins.length; j++ ) {
        p(sb,"cnt" ,cntW).append('/');
        p(sb,"min" ,mmmW).append('/');
        p(sb,"mean",mmmW).append('/');
        p(sb,"max" ,mmmW).append('/');
        p(sb,"var" ,varW).append(colPad);
      }
      sb.append('\n');
      Formatter f = new Formatter(sb);
      for( int i=0; i<_bins[0].length; i++ ) {
        for( int j=0; j<_bins.length; j++ ) {
          p(sb,Long.toString(_bins[j][i]),cntW).append('/');
          p(sb,              _mins[j][i] ,mmmW).append('/');
          p(sb,               mean(j, i) ,mmmW).append('/');
          p(sb,              _maxs[j][i] ,mmmW).append('/');
          p(sb,               var (j, i) ,varW).append(colPad);
        }
        sb.append('\n');
      }
      return sb.toString();
    }
    static private StringBuilder p(StringBuilder sb, String s, int w) {
      return sb.append(Log.fixedLength(s,w));
    }
    static private StringBuilder p(StringBuilder sb, double d, int w) {
      String s = Double.isNaN(d) ? "NaN" :
        ((d==Double.MAX_VALUE || d==Double.MIN_VALUE) ? " -" : 
         Double.toString(d));
      if( s.length() > w ) return sb.append(String.format("%4.0f",d));
      return p(sb,s,w);
    }
  }
}

