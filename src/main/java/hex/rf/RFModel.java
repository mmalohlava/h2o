package hex.rf;

import java.util.Arrays;
import java.util.Random;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.binary.StringUtils;

import water.*;
import water.Job.Progress;
import water.api.Constants;
import water.util.*;
import water.util.Log.Tag.Sys;

import com.google.gson.*;

/**
 * A model is an ensemble of trees that can be serialized and that can be used
 * to classify data.
 */
public class RFModel extends Model implements Cloneable, Progress {
  /** Number of features these trees are built for */
  public int _features;
  /** Sampling strategy used for model */
  public Sampling.Strategy _samplingStrategy;
  /** Sampling rate used when building trees. */
  public float _sample;
  /** Strata sampling rate used for local-node strata-sampling */
  public float[] _strataSamples;
  /** Number of split features defined by user. */
  public int _splitFeatures;
  /** Number of computed split features per node - number of split features can differ for each node.
   * However, such difference would point to a problem with data distribution. */
  public int[] _nodesSplitFeatures;
  /** Stop splitting when node has less then given number of records */
  public int _nodesize;
  /** Number of keys the model expects to be built for it */
  public int _totalTrees;
  /** All the trees in the model */
  public Key[]     _tkeys;
  /** Local forests produced by nodes */
  public Key[][]   _localForests;
  /** Refine queues */
  public Key[][]   _refineQueue;
  /** Total time in seconds to produce model */
  public long      _time;

  public static final String KEY_PREFIX = "__RFModel_";

  /** A RandomForest Model
   * @param treeskey    a key of keys of trees
   * @param classes     the number of response classes
   * @param data        the dataset
   */
  public RFModel(Key selfKey, int[] cols, Key dataKey, Key[] tkeys, int features, Sampling.Strategy samplingStrategy, float sample, float[] strataSamples, int splitFeatures, int totalTrees, int nodesize) {
    super(selfKey, cols, dataKey);
    _features = features;
    _sample = sample;
    _splitFeatures = splitFeatures;
    _totalTrees = totalTrees;
    _tkeys = tkeys;
    _strataSamples = strataSamples;
    _samplingStrategy = samplingStrategy;
    _nodesSplitFeatures = new int[H2O.CLOUD.size()];
    _localForests       = new Key[H2O.CLOUD.size()][];
    _refineQueue        = new Key[H2O.CLOUD.size()][];
    _nodesize = nodesize;
    for(int i=0;i<H2O.CLOUD.size();i++) _localForests[i] = new Key[0];
    for(int i=0;i<H2O.CLOUD.size();i++) _refineQueue[i] = new Key[0];
    for( Key tkey : _tkeys ) assert DKV.get(tkey)!=null;
  }

  public RFModel(Key selfKey, String [] colNames, String[] classNames, Key[] tkeys, int features, float sample, int nodesize) {
    super(selfKey,colNames,classNames);
    _features       = features;
    _sample         = sample;
    _splitFeatures  = features;
    _totalTrees     = tkeys.length;
    _tkeys          = tkeys;
    _samplingStrategy   = Sampling.Strategy.RANDOM;
    _nodesSplitFeatures = new int[H2O.CLOUD.size()];
    _localForests       = new Key[H2O.CLOUD.size()][];
    _refineQueue        = new Key[H2O.CLOUD.size()][];
    _nodesize = nodesize;
    for(int i=0;i<H2O.CLOUD.size();i++) _localForests[i] = new Key[0];
    for(int i=0;i<H2O.CLOUD.size();i++) _refineQueue[i] = new Key[0];
    for( Key tkey : _tkeys ) assert DKV.get(tkey)!=null;
    assert classes() > 0;
  }

  /** Empty constructor for deserialization */
  public RFModel() {}

  @Override protected RFModel clone() {
    try {
      return (RFModel) super.clone();
    } catch( CloneNotSupportedException cne ) {
      throw Log.err(Sys.RANDF, "", H2O.unimpl());
    }
  }

  static public RFModel make(RFModel old, Key tkey, int nodeIdx, int refineNodeIdx) {
    RFModel m = old.clone();
    m._tkeys = Arrays.copyOf(old._tkeys,old._tkeys.length+1);
    m._tkeys[m._tkeys.length-1] = tkey;
    // updating local forests
    m._localForests[nodeIdx] = Arrays.copyOf(old._localForests[nodeIdx],old._localForests[nodeIdx].length+1);
    m._localForests[nodeIdx][m._localForests[nodeIdx].length-1] = tkey;
    // updating refine queue
    m._refineQueue[refineNodeIdx] = Arrays.copyOf(m._refineQueue[refineNodeIdx], m._refineQueue[refineNodeIdx].length+1);
    m._refineQueue[refineNodeIdx][m._refineQueue[refineNodeIdx].length-1] = tkey;

    return m;
  }

  static public RFModel updateRQ(RFModel old, Key tKey, int refineNodeIdx) {
    RFModel m = old.clone();
    // updating refine queue
    m._refineQueue[refineNodeIdx] = Arrays.copyOf(m._refineQueue[refineNodeIdx], m._refineQueue[refineNodeIdx].length+1);
    m._refineQueue[refineNodeIdx][m._refineQueue[refineNodeIdx].length-1] = tKey;

    return m;
  }

  // Make a random RF key
  public static final Key makeKey() {
    return Key.make(KEY_PREFIX + Key.make());
  }

  /** The number of trees in this model. */
  public int treeCount() { return _tkeys.length; }
  public int size()      { return _tkeys.length; }
  public int classes()   { ValueArray.Column C = response();  return (int)(C._max - C._min + 1); }

  @Override public float progress() {
    return size() / (float) _totalTrees;
  }

  public String name(int atree) {
    if( atree == -1 ) atree = size();
    assert atree <= size();
    return _selfKey.toString() + "[" + atree + "]";
  }

  /** Return the bits for a particular tree */
  public byte[] tree(int tree_id) {
    return DKV.get(_tkeys[tree_id]).memOrLoad();
  }

  /** Bad name, I know. But free all internal tree keys. */
  public void deleteKeys() {
    for( Key k : _tkeys )
      UKV.remove(k);
  }

  /**
   * Classify a row according to one particular tree.
   * @param tree_id  the number of the tree to use
   * @param chunk    the chunk we are using
   * @param row      the row number in the chunk
   * @param modelDataMap  mapping from model/tree columns to data columns
   * @return the predicted response class, or class+1 for broken rows
   */
  public short classify0(int tree_id, ValueArray data, AutoBuffer chunk, int row, int modelDataMap[], short badrow) {
    return Tree.classify(new AutoBuffer(tree(tree_id)), data, chunk, row, modelDataMap, badrow);
  }

  private void vote(ValueArray data, AutoBuffer chunk, int row, int modelDataMap[], int[] votes) {
    int numClasses = classes();
    assert votes.length == numClasses + 1/* +1 to catch broken rows */;
    for( int i = 0; i < treeCount(); i++ )
      votes[classify0(i, data, chunk, row, modelDataMap, (short) numClasses)]++;
  }

  public short classify(ValueArray data, AutoBuffer chunk, int row, int modelDataMap[], int[] votes, double[] classWt, Random rand ) {
    // Vote all the trees for the row
    vote(data, chunk, row, modelDataMap, votes);
    return classify(votes, classWt, rand);
  }

  public short classify(int[] votes, double[] classWt, Random rand) {
    // Scale the votes by class weights: it as-if rows of the weighted classes
    // were replicated many times so get many votes.
    if( classWt != null )
      for( int i=0; i<votes.length-1; i++ )
      votes[i] = (int) (votes[i] * classWt[i]);
    // Tally results
    int result = 0;
    int tied = 1;
    for( int i = 1; i < votes.length - 1; i++ )
      if( votes[i] > votes[result] ) { result=i; tied=1; }
      else if( votes[i] == votes[result] ) { tied++; }
    if( tied == 1 ) return (short) result;
    // Tie-breaker logic
    int j = rand == null ? 0 : rand.nextInt(tied); // From zero to number of tied classes-1
    int k = 0;
    for( int i = 0; i < votes.length - 1; i++ )
      if( votes[i]==votes[result] && (k++ >= j) )
        return (short)i;
    throw H2O.unimpl();
  }

  // The seed for a given tree
  long seed(int ntree) {
    return UDP.get8(tree(ntree), 4);
  }

  // Lazy initialization of tree leaves, depth
  private transient Counter _tl, _td;

  /** Internal computation of depth and number of leaves. */
  public void find_leaves_depth() {
    if( _tl != null ) return;
    _td = new Counter();
    _tl = new Counter();
    for( Key tkey : _tkeys ) {
      long dl = Tree.depth_leaves(new AutoBuffer(DKV.get(tkey).memOrLoad()));
      _td.add((int) (dl >> 32));
      _tl.add((int) dl);
    }
  }
  public Counter leaves() { find_leaves_depth(); return _tl; }
  public Counter depth()  { find_leaves_depth(); return _td; }

  /** Return the random seed used to sample this tree. */
  public long getTreeSeed(int i) {  return Tree.seed(tree(i)); }

  /** Single row scoring, on properly ordered data */
  protected double score0(double[] data) {
    int numClasses = classes();
    int votes[] = new int[numClasses + 1/* +1 to catch broken rows */];
    for( int i = 0; i < treeCount(); i++ )
      votes[(int) Tree.classify(new AutoBuffer(tree(i)), data, numClasses)]++;
    return classify(votes, null, null) + response()._min;
  }

  /** Single row scoring, on a compatible ValueArray (when pushed throw the mapping) */
  protected double score0( ValueArray data, int row) { throw H2O.unimpl(); }

  /** Bulk scoring API, on a compatible ValueArray (when pushed throw the mapping) */
  protected double score0(ValueArray data, AutoBuffer ab, int row_in_chunk) { throw H2O.unimpl(); }

  @Override public JsonObject toJson() {
    JsonObject res = new JsonObject();
    res.addProperty(Constants.VERSION, H2O.VERSION);
    res.addProperty(Constants.TYPE, RFModel.class.getName());
    res.addProperty("features", _features);
    res.addProperty("sampling_strategy", _samplingStrategy.name());
    res.addProperty("sample", _sample);
    JsonArray vals = new JsonArray();
    for(float f:_strataSamples)
      vals.add(new JsonPrimitive(f));
    res.add("strataSamples", vals);
    res.addProperty("split_features", _splitFeatures);
    vals = new JsonArray();
    for(int i:_nodesSplitFeatures)
      vals.add(new JsonPrimitive(i));
    res.add("nodesSplitFeatures", vals);
    res.addProperty("totalTrees", _totalTrees);
    res.addProperty("time", _time);
    vals = new JsonArray();
    for( Key tkey : _tkeys ) {
      byte[] b = Base64.encodeBase64(DKV.get(tkey).memOrLoad(), false);
      vals.add(new JsonPrimitive(StringUtils.newStringUtf8(b)));
    }
    res.add("trees", vals);
    return res;
  }
}
