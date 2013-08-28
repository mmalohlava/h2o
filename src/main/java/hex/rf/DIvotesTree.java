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

  public static class OOBEE {
    int[] _misrows;
    int _totalRows;


    public OOBEE(int[] misrows, int totalRows) { _misrows = misrows; _totalRows = totalRows; }
    public double error() { return (double) _misrows.length / _totalRows; }
  }

  public static OOBEE voteOOB(int k, Key dataKey, Key modelKey, int classcol, Key[] localChunks, int[] chunkRowMapping, Key[] trees, Key[] oobKeys) {
    OOBVotingTask voting = new OOBVotingTask(k, trees, oobKeys, dataKey, modelKey, classcol, chunkRowMapping);
    voting.invoke(localChunks);

    System.err.println("MISSED ROWS: " + Arrays.toString(voting._misrows));
    System.err.println("TOTAL  ROWS: " + voting._totalRows);

    return new OOBEE(voting._misrows, voting._totalRows);
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
        long treeSeed = Tree.seed(treeBits);
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
}
