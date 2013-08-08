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

  final ChunksRowsFilter _crf;

  public RefinedTreeMarkAndLogRows(Job job, byte round, Key origTreeKey, AutoBuffer serialTree,
      byte treeProducerIdx, int treeIdx, int treeId, long seed, Data data, int maxDepth, StatType stat,
      int numSplitFeatures, int exclusiveSplitLimit, Sampling sampler, int verbose, int nodesize) {
    super(SAVE_CHUNKS_FILTER, job, round, origTreeKey, serialTree, treeProducerIdx, treeIdx, treeId, seed, data, maxDepth, stat,
        numSplitFeatures, exclusiveSplitLimit, sampler, verbose, nodesize);
    _crf = new ChunksRowsFilter(data._dapt._ary, data._dapt._homeKeys);
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

  // requires preserving of chunks keys ordering - from data load to this code
  private final Key chunk(Data data, int idx) { return data._dapt._homeKeys[Math.min(idx, data._dapt._homeKeys.length-1)]; }

  final void identifyChunks(Data data) {
    Iterator<Row> it = data.iterator();
    while (it.hasNext()) {
      Row row = it.next();
      int cidx = row._index / data._dapt._ary.rpc(0); // FIXME: this is not exactly right since chunks can has different sizes...
      Key chunk = chunk(data, cidx);
      _crf.addRow(chunk, row._index);
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

  public static final TRunnable<RefinedTree> SAVE_CHUNKS_FILTER = new TRunnable<RefinedTree>() {
    @Override public void run(RefinedTree t) {
      RefinedTreeMarkAndLogRows r = (RefinedTreeMarkAndLogRows)t;

      // Close chunks/rows filter
      r._crf.close();
      System.err.println("Node " + r._producerIdx + " needs " + r._crf.toString() + " from " + H2O.SELF.index() );
      Key key = ChunksRowsFilter.makeKey(r._origTreeKey, r._producerIdx, (byte) H2O.SELF.index());
      UKV.put(key, r._crf);
      System.err.println("Saved filter into " + key);
    }
  };

  public static class ChunksRowsFilter extends Iced {

    public static final int INIT_SIZE = 1024;
    Key[] _chunks;
    int[][] _rows;

    transient int[] _idx;
    transient int[] _startRow;

    public ChunksRowsFilter(ValueArray ary, Key chunks[]) { this(ary, chunks, INIT_SIZE); }
    public ChunksRowsFilter(ValueArray ary, Key chunks[], int size) {
      _chunks = Arrays.copyOf(chunks, chunks.length);
      _idx = new int[_chunks.length];
      _rows = new int[_chunks.length][];
      for (int i=0; i<_chunks.length; i++) _rows[i] = new int[size];
      _startRow = new int[_chunks.length];
      _startRow[0] = 0;
      for (int i=1; i<_chunks.length; i++) _startRow[i] += _startRow[i-1] + ary.rpc(ValueArray.getChunkIndex(_chunks[i-1]));
    }

    public final int addChunk(Key c) {
      for (int i=0; i<_chunks.length;i++) if (_chunks[i].equals(c)) return i;
      assert false : "This should not happen!";
      return -1;
    }

    public final void addRow(Key chunk, int row) {
      int cidx = addChunk(chunk);
      _rows[cidx][_idx[cidx]++] = row - _startRow[cidx];
      if (_idx[cidx] == _rows[cidx].length)
        _rows[cidx] = Arrays.copyOf(_rows[cidx], 2*_rows[cidx].length);
    }

    public void close() {
      for (int i=0; i<_rows.length; i++) {
        if (_rows[i].length > _idx[i]) _rows[i] = Arrays.copyOf(_rows[i], _idx[i]);
        Arrays.sort(_rows[i]);
      }
    }

    public static Key makeKey(Key tree, byte producerIdx, byte refinerIdx) {
      byte[] code = Arrays.copyOfRange(tree._kb,2+6+4,tree._kb.length+2);
      code[code.length-2] = producerIdx;
      code[code.length-1] = refinerIdx;
      boolean visible = true;
      return visible ? Key.make(code) : Key.make(code, (byte)1, Key.DFJ_INTERNAL_USER, H2O.CLOUD._memary[producerIdx]);
    }

    @Override public String toString() {
      StringBuilder sb = new StringBuilder("ChunkRowsFilter {\n");
      for (int i=0; i<_chunks.length; i++) {
        sb.append(" - from chunk ").append(_chunks[i].toString()).append(" located on this node needs to load ").append(_rows[i].length).append(" rows.\n");
      }
      sb.append('}');
      return sb.toString();
    }
  }
}
