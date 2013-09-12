package hex.rf;


import java.util.Arrays;

import hex.rf.Data.Row;
import hex.rf.RFDIvotes.OOBSample;
import water.*;
import water.ValueArray.Column;
import water.util.*;
import water.util.Log.Tag.Sys;

public class DIvotesTree extends Tree {

  final int[] _sample;

  public DIvotesTree(Job job, Data data, byte producerId, int maxDepth, StatType stat, int numSplitFeatures, long seed,
      int treeId, int exclusiveSplitLimit, Sampling sampler, int verbose, int[] sample) {
    super(job, data, producerId, maxDepth, stat, numSplitFeatures, seed, treeId, exclusiveSplitLimit, sampler, verbose);
    _sample = sample;
  }

  @Override public void compute2() {
    if(!_job.cancelled()) {
      Timer timer    = new Timer();
      _stats[0]      = new ThreadLocal<Statistic>();
      _stats[1]      = new ThreadLocal<Statistic>();
      Data         d = Sampling.sample(_data, _sample);
      Statistic left = getStatistic(0, d, _seed, _exclusiveSplitLimit);
      // calculate the split
      for( Row r : d ) left.addQ(r);
      left.applyClassWeights();   // Weight the distributions
      Statistic.Split spl = left.split(d, false);
      _tree = spl.isLeafNode()
        ? new LeafNode(_data.unmapClass(spl._split), d.rows())
        : new FJBuild (spl, d, 0, _seed).compute();

      _stats = null; // GC

      // Atomically improve the Model as well

      _thisTreeKey = toKey();
      appendKey(_job.dest(),_thisTreeKey);
      StringBuilder sb = new StringBuilder("[RF] Tree : ").append(_data_id+1);
      sb.append(" d=").append(_tree.depth()).append(" leaves=").append(_tree.leaves()).append(" done in ").append(timer).append('\n');
      Log.debug(Sys.RANDF,_tree.toString(sb,  _verbose > 0 ? Integer.MAX_VALUE : 200).toString());
    }
    // Wait for completation
    tryComplete();
  }

  /** Error Estimate */
  public static class EE {
    int[] _misrows;
    int _totalRows;

    public EE(int[] misrows, int totalRows) { _misrows = misrows; _totalRows = totalRows; }
    public double error() { return (double) _misrows.length / _totalRows; }
    @Override public String toString() {
      return _totalRows +" / " + Arrays.toString(_misrows);
    }
  }

  public static EE voteOOB(int k, Key modelKey, Key dataKey, int classcol, Key[] localChunks, int[] chunkRowMapping, Key[] trees, Key[] oobKeys) {
    OOBVotingTask voting = new OOBVotingTask(k, trees, oobKeys, dataKey, modelKey, classcol, chunkRowMapping);
    voting.invoke(localChunks); // invoke only on specified chunks

    return new EE(voting._misrows, voting._totalRows);
  }

  public static EE vote(int k, Key modelKey, Key testDataKey, int classcol, Key[] trees, int[] chunkRowMapping) {
    VotingTask voting = new VotingTask(k, trees, testDataKey, modelKey, classcol, chunkRowMapping);
    // invoke on test data
    voting.invoke(testDataKey);

    return new EE(voting._misrows, voting._totalRows);
  }

  public static int[][][] diversity(Key modelKey, Key testDataKey, int classcol, int[] chunkRowsMapping) {
    RFModel rfModel = UKV.get(modelKey);
    EE[] missesPerNodes = new EE[rfModel._localForests.length];
    // dummy vote per node
    for (int node=0; node<rfModel._localForests.length; node++) {
      Key[] nodeForest = rfModel._localForests[node];
      missesPerNodes[node] = vote(nodeForest.length-1, modelKey, testDataKey, classcol, nodeForest, chunkRowsMapping);
    }
    //for (int i=0; i<missesPerNodes.length;i++) System.err.println(missesPerNodes[i]);
    int N = missesPerNodes.length;
    int[][][] result = new int[N*N][][];
    for (int i=0; i<N; i++) {
      for (int j=0; j<N; j++) {
        int pidx = i*N + j;
        if (i==j) result[pidx] = null;
        else {
          int[] ti = missesPerNodes[i]._misrows;
          int[] tj = missesPerNodes[j]._misrows;
          int a=0,b=0,c=0,d=0; // a = both correct, b=i-correct,j-wrong, c=i-wrong,j-correct, d=both wrong
          assert missesPerNodes[i]._totalRows == missesPerNodes[j]._totalRows;
          int iidx = 0, jidx = 0;
          int lastMiss = -1;
          int total = missesPerNodes[i]._totalRows;
          while (iidx < ti.length || jidx < tj.length) {
            if (ti[iidx] == tj[jidx]) { // both misses
              d++;
              a += ti[iidx] - lastMiss - 1; // agree on previous ( lastMiss, ti[iidx] ) votes
              lastMiss=ti[iidx];
              iidx++; jidx++;
            } else if (ti[iidx] < tj[jidx]) { // ti-misses, tj is correct for ti[iidx]
              c++;
              a += ti[iidx] - lastMiss - 1;
              lastMiss = ti[iidx];
              iidx++;
            } else { // ti is correct, but tj is wrong
              b++;
              a += tj[jidx] - lastMiss - 1;
              lastMiss = tj[jidx];
              jidx++;
            }
            if (iidx == ti.length) {
              a += total-lastMiss-1; // optimistically agree on all the rest
              while (jidx < tj.length) { a--; b++; jidx++; } // compute all tj-wrong rows, but decrease optimistically computed a
            }
            if (jidx == tj.length) {
              a += total-lastMiss-1; // optimistically agree on all the rest
              while (iidx < ti.length) { a--; c++; iidx++; } // compute all ti-wrong rows, but decrease optimistically computed a
            }
          }
          int[][] tmpR = result[pidx] = new int[2][2];
          tmpR[0][0] = a; tmpR[1][1] = d;
          tmpR[0][1] = b; tmpR[1][0] = c;
        }
      }
    }
    return result;
  }

  public static Disagreement disagreement(Key modelKey, Key dataKey, int classcol) {
    ErrorTrackingTask t = new ErrorTrackingTask(dataKey, modelKey, classcol);
    t.invoke(dataKey);

    RFModel rfModel = UKV.get(modelKey);
    int nodes = rfModel._localForests.length;
    int[] treesPerNode = new int[nodes];
    for (int i=0; i<nodes; i++) treesPerNode[i] = rfModel._localForests[i].length;
    ValueArray ary = UKV.get(dataKey);
    int chunks = (int) ary.chunks();
    int[] linesPerChunk = new int[chunks];
    for (int i=0; i<chunks; i++) linesPerChunk[i] = ary.rpc(i);
    return new Disagreement(t._home, t._nodeErrPerChunk, nodes, treesPerNode, linesPerChunk );
  }

  public static class Disagreement {
    public int[]   _chunkHomes;
    public int[][] _nodeErrPerChunk;
    public int _nodes;
    public int[] _treesPerNode;
    public int[] _linesPerChunk;
    public Disagreement(int[] chunkHomes, int[][] nodeErrPerChunk, int nodes, int[] treesPerNode, int[] linesPerChunk) {
      _chunkHomes = chunkHomes; _nodeErrPerChunk = nodeErrPerChunk; _nodes = nodes;
      _treesPerNode = treesPerNode;
      _linesPerChunk = linesPerChunk;
    }
  }

  /** Perform voting over OOB instances.
   */
  public static class OOBVotingTask extends MRTask<OOBVotingTask> {
    final int _k;
    /* @IN */
    final Key[] _trees;
    /* @IN */
    final Key[] _oobKeys;
    /* @IN */
    final Key   _dataKey;
    /* @IN */
    final Key   _modelKey;
    /* @IN */
    final int   _classcol;
    /* @IN */
    final int[] _chunkRowMapping;

    /* @OUT misclassified rows. */
    int[] _misrows;
    /* @OUT */
    int _totalRows;

    transient ValueArray _data;
    transient int _N; // number of classes in response column
    transient int _cmin;
    transient int[] _modelColMap;

    public OOBVotingTask(int k, Key[] trees, Key[] oobKeys, Key dataKey, Key modelKey, int classcol, int[] chunkRowMapping) {
      super();
      _k = k;
      _trees = trees;
      _oobKeys = oobKeys;
      _dataKey = dataKey;
      _modelKey = modelKey;
      _classcol = classcol;
      _chunkRowMapping = chunkRowMapping;
    }

    @Override public void init() {
      super.init();
      _data = UKV.get(_dataKey);
      RFModel model  = UKV.get(_modelKey);
      _modelColMap = model.columnMapping(_data.colNames());
      // Response parameters (no values alignment !!)
      Column[] cols = _data._cols;
      Column respCol = cols[_classcol];
      _N = (int) respCol.numDomainSize();
      _cmin = (int) respCol._min;
    }

    @Override public void map(Key ckey) {
      final AutoBuffer cdata = _data.getChunk(ckey); // data stored in chunk
      final int cIdx = (int) ValueArray.getChunkIndex(ckey); // chunk index
      final int rows = _data.rpc(cIdx); // rows in chunk
      final int initRow = _chunkRowMapping[cIdx];
      final int ntrees  = _k+1;

      int[] votes = new int[rows];
      byte[] usedRows = new byte[rows];
      // Sample

      for( int ntree=0; ntree < ntrees; ntree++ ) {
        byte[] treeBits = DKV.get(_trees[ntree]).memOrLoad(); // FIXME: cache it
        byte producerId = Tree.producerId(treeBits);
        assert producerId == H2O.SELF.index() : "Ups. Tree is voted on other node?";

        OOBSample oobSample = UKV.get(_oobKeys[ntree]);
        assert oobSample._k == ntree;
        int[] sample = oobSample._oob;

        // Find a start row idx for this chunk
        int startRowIdx = 0;
        while (startRowIdx < sample.length && sample[startRowIdx] < initRow) startRowIdx++;
        ROWS: for ( int i=startRowIdx; i<sample.length; i++) {
          int actualRow = sample[i];
          if (actualRow >= initRow+rows) break;
          int row = actualRow - initRow;
          // Bail out of broken rows with NA in class column.
          // Do not skip yet the rows with NAs in the rest of columns
          if( _data.isNA(cdata, row, _classcol)) continue ROWS;
          // Mark row as used
          usedRows[row] = 1;
          // Make a prediction for given tree and out-of-bag row
          int prediction = Tree.classify(new AutoBuffer(treeBits), _data, cdata, row, _modelColMap, (short)_N);
          if( prediction >= _N ) continue ROWS; // Junk row cannot be predicted
          int dataResponse = (int) (_data.data(cdata, row, _classcol)) - _cmin;
          if (prediction == dataResponse)
            votes[row]++; // Vote the row
        }
        treeBits = null;
      }
      //System.err.println("Votes: " + Arrays.toString(votes));
      // Collect mis-predicted rows
      int majority = ntrees / 2 + 1; // 1 node, majority = 1, 2 nodes => majority 2, 3nodes = majority 2
      int mispredRows = 0;
      _totalRows = Utils.sum(usedRows);
      for (int r=0; r<votes.length; r++) if (usedRows[r]>0 && votes[r] < majority) mispredRows++;
      _misrows = new int[mispredRows];
      for (int r=0,cnt=0; r<votes.length;r++)
        if (usedRows[r]>0 && votes[r] < majority) _misrows[cnt++] = r + initRow;
    }

    @Override public void reduce(OOBVotingTask drt) {
      _totalRows += drt._totalRows;
      if (drt._misrows!=null) {
        // do sorted merge
        _misrows = Utils.join(_misrows, drt._misrows);
      }
    }
  }

  public static class VotingTask extends MRTask<VotingTask> {
    final int _k;
    /* @IN */
    final Key[] _trees;
    /* @IN */
    final Key   _dataKey;
    /* @IN */
    final Key   _modelKey;
    /* @IN */
    final int   _classcol;
    /* @IN */
    final int[] _chunkRowMapping;

    /* @OUT misclassified rows. */
    int[] _misrows;
    /* @OUT */
    int _totalRows;

    transient ValueArray _data;
    transient int _N; // number of classes in response column
    transient int _cmin;
    transient int[] _modelColMap;

    public VotingTask(int k, Key[] trees, Key dataKey, Key modelKey, int classcol, int[] chunkRowMapping) {
      super();
      _k = k;
      _trees = trees;
      _dataKey = dataKey;
      _modelKey = modelKey;
      _classcol = classcol;
      _chunkRowMapping = chunkRowMapping;
    }

    @Override public void init() {
      super.init();
      _data = UKV.get(_dataKey);
      RFModel model  = UKV.get(_modelKey);
      _modelColMap = model.columnMapping(_data.colNames());
      // Response parameters (no values alignment !!)
      Column[] cols = _data._cols;
      Column respCol = cols[_classcol];
      _N = (int) respCol.numDomainSize();
      _cmin = (int) respCol._min;
    }

    @Override public void map(Key ckey) {
      System.out.println("DIvotesTree.VotingTask.map(): " + ckey);
      final AutoBuffer cdata = _data.getChunk(ckey); // data stored in chunk
      final int cIdx = (int) ValueArray.getChunkIndex(ckey); // chunk index
      final int rows = _data.rpc(cIdx); // rows in chunk
      final int initRow = _chunkRowMapping[cIdx];
      final int ntrees  = _k+1;

      int[] votes = new int[rows];

      for( int ntree=0; ntree < ntrees; ntree++ ) {
        byte[] treeBits = DKV.get(_trees[ntree]).memOrLoad(); // FIXME: cache it

        // Find a start row idx for this chunk
        ROWS: for ( int row=0; row<rows; row++) {
          // Bail out of broken rows with NA in class column.
          // Do not skip yet the rows with NAs in the rest of columns
          if( _data.isNA(cdata, row, _classcol)) continue ROWS;
          // Make a prediction for given tree and out-of-bag row
          int prediction = Tree.classify(new AutoBuffer(treeBits), _data, cdata, row, _modelColMap, (short)_N);
          if( prediction >= _N ) continue ROWS; // Junk row cannot be predicted
          int dataResponse = (int) (_data.data(cdata, row, _classcol)) - _cmin;
          if (prediction == dataResponse)
            votes[row]++; // Vote the row
        }
        treeBits = null;
      }

      int majority = ntrees / 2 + 1; // 1 node, majority = 1, 2 nodes => majority 2, 3nodes = majority 2
      int mispredRows = 0;
      for (int r=0; r<votes.length; r++) if (votes[r] < majority) mispredRows++;
      _misrows = new int[mispredRows];
      for (int r=0,cnt=0; r<votes.length;r++)
        if (votes[r] < majority) _misrows[cnt++] = r + initRow;
      _totalRows += rows; // RPC
    }

    @Override public void reduce(VotingTask drt) {
      _totalRows += drt._totalRows;
      if (drt._misrows!=null) {
        // do sorted merge
        _misrows = Utils.join(_misrows, drt._misrows);
      }
    }
  }

  public static class ErrorTrackingTask extends MRTask<ErrorTrackingTask> {

    /* @IN */
    final Key   _dataKey;
    /* @IN */
    final Key   _modelKey;
    /* @IN */
    final int   _classcol;

    /* @OUT number of erros per node forest per chunk */
    int[][] _nodeErrPerChunk;
    /* @OUT */
    int[]   _home; // home node for chunk

    transient ValueArray _data;
    transient RFModel  _model;
    transient int _N; // number of classes in response column
    transient int _cmin;
    transient int[] _modelColMap;

    public ErrorTrackingTask(Key dataKey, Key modelKey, int classcol) {
      super();
      _dataKey = dataKey;
      _modelKey = modelKey;
      _classcol = classcol;
    }

    @Override public void init() {
      super.init();
      _data = UKV.get(_dataKey);
      _model  = UKV.get(_modelKey);
      _modelColMap = _model.columnMapping(_data.colNames());
      // Response parameters (no values alignment !!)
      Column[] cols = _data._cols;
      Column respCol = cols[_classcol];
      _N = (int) respCol.numDomainSize();
      _cmin = (int) respCol._min;
    }

    @Override public void map(Key ckey) {
      final AutoBuffer cdata = _data.getChunk(ckey); // data stored in chunk
      final int cIdx = (int) ValueArray.getChunkIndex(ckey); // chunk index
      final int rows = _data.rpc(cIdx); // rows in chunk
      final int nodes = _model._localForests.length;
      final int chunks = (int) _data.chunks();
      _nodeErrPerChunk = new int[chunks][];
      _home = new int[chunks];

      _home[cIdx] = ckey.home_node().index();
      _nodeErrPerChunk[cIdx] = new int[nodes];

      for (int nodeIdx=0; nodeIdx<nodes; nodeIdx++) {
        Key[] trees = _model._localForests[nodeIdx];
        final int ntrees  = trees.length;

        int[] votes = new int[rows];

        for( int ntree=0; ntree < ntrees; ntree++ ) {
          byte[] treeBits = DKV.get(trees[ntree]).memOrLoad(); // FIXME: cache it

          ROWS: for ( int row=0; row<rows; row++) {
            // Bail out of broken rows with NA in class column.
            // Do not skip yet the rows with NAs in the rest of columns
            if( _data.isNA(cdata, row, _classcol)) continue ROWS;
            // Make a prediction for given tree and out-of-bag row
            int prediction = Tree.classify(new AutoBuffer(treeBits), _data, cdata, row, _modelColMap, (short)_N);
            if( prediction >= _N ) continue ROWS; // Junk row cannot be predicted
            int dataResponse = (int) (_data.data(cdata, row, _classcol)) - _cmin;
            if (prediction == dataResponse)
              votes[row]++; // Vote the row
          }
          treeBits = null;
        }

        int majority = ntrees / 2 + 1; // 1 node, majority = 1, 2 nodes => majority 2, 3nodes = majority 2
        int mispredRows = 0;
        for (int r=0; r<votes.length; r++) if (votes[r] < majority) mispredRows++;
        _nodeErrPerChunk[cIdx][nodeIdx] = mispredRows;
      }
    }

    @Override public void reduce(ErrorTrackingTask drt) {
      if (drt == null) return;
      if (_home == null) {
        assert _nodeErrPerChunk == null;
        _home = drt._home; _nodeErrPerChunk = drt._nodeErrPerChunk;
      } else if (drt._home!=null) {
        assert drt._nodeErrPerChunk!=null;
        assert drt._home.length == _home.length;
        assert drt._nodeErrPerChunk.length == _nodeErrPerChunk.length;
        for (int i=0; i<drt._nodeErrPerChunk.length; i++) {
          if (drt._nodeErrPerChunk[i]!=null) {
            assert _nodeErrPerChunk[i] == null;
            _nodeErrPerChunk[i] = drt._nodeErrPerChunk[i];
            _home[i] = drt._home[i];
          }
        }
      }
    }
  }

}
