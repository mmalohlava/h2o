package hex.rf;

import hex.rf.ConfusionTask.CMFinal;
import hex.rf.DRF.DRFJob;
import hex.rf.DRF.DRFParams;
import hex.rf.Tree.StatType;
import hex.rng.H2ORandomRNG.RNGKind;

import java.io.File;
import java.util.*;

import jsr166y.ForkJoinTask;
import water.*;
import water.Timer;
import water.util.*;
import water.util.Log.Tag.Sys;

/**
 * A RandomForest can be used for growing or validation. The former starts with a known target number of trees,
 * the latter is incrementally populated with trees as they are built.
 * Validation and error reporting is not supported when growing a forest.
 */
public class RandomForest {

  /** Seed initializer.
   *  Generated by:  cat /dev/urandom | tr -dc '0-9a-f' | fold -w 16| head -n 1' */
  static final long ROOT_SEED_ADD  = 0x026244fd935c5111L;
  static final long TREE_SEED_INIT = 0x1321e74a0192470cL;

  /** Build random forest for data stored on this node. */
  public static void build(
                      final Job job,
                      final DRFParams drfParams,
                      final Data data,
                      int ntrees,
                      int numSplitFeatures,
                      int[] rowsPerChunks) {
    Timer  t_alltrees = new Timer();
    Tree[] trees      = new Tree[ntrees];
    Log.debug(Sys.RANDF,"Building "+ntrees+" trees");
    Log.debug(Sys.RANDF,"Number of split features: "+ numSplitFeatures);
    Log.debug(Sys.RANDF,"Starting RF computation with "+ data.rows()+" rows ");

    Random  rnd = Utils.getRNG(data.seed() + ROOT_SEED_ADD);
    Sampling sampler = createSampler(drfParams, rowsPerChunks);
    byte producerId = (byte) H2O.SELF.index();
    for (int i = 0; i < ntrees; ++i) {
      long treeSeed = rnd.nextLong() + TREE_SEED_INIT; // make sure that enough bits is initialized
      trees[i] = new Tree(job, data, producerId, drfParams._depth, drfParams._stat, numSplitFeatures, treeSeed,
                          i, drfParams._exclusiveSplitLimit, sampler, drfParams._verbose);
      if (!drfParams._parallel)   ForkJoinTask.invokeAll(new Tree[]{trees[i]});
    }

    if(drfParams._parallel) DRemoteTask.invokeAll(trees);
    Log.debug(Sys.RANDF,"All trees ("+ntrees+") done in "+ t_alltrees);
  }

  static Sampling createSampler(final DRFParams params, int[] rowsPerChunks) {
    switch(params._samplingStrategy) {
    case RANDOM          : return new Sampling.Random(params._sample, rowsPerChunks);
    case STRATIFIED_LOCAL:
      float[] ss = new float[params._strataSamples.length];
      for (int i=0;i<ss.length;i++) ss[i] = params._strataSamples[i] / 100.f;
      return new Sampling.StratifiedLocal(ss, params._numrows);
    case RANDOM_WITH_REPLACEMENT: return new Sampling.RWR(params._sample, rowsPerChunks);
    default:
      assert false : "Unsupported sampling strategy";
      return null;
    }
  }

  public static class OptArgs extends Arguments.Opt {
    String  file          = "smalldata/poker/poker-hand-testing.data";
    String  rawKey;
    String  parsedKey;
    String  validationFile;
    String  h2oArgs;
    int     ntrees        = 10;
    int     depth         = Integer.MAX_VALUE;
    int     sample        = 67;
    int     binLimit      = 1024;
    int     classcol      = -1;
    int     features      = -1;
    int     parallel      = 1;
    boolean outOfBagError = true;
    boolean stratify      = false;
    String  strata;
    String  weights;
    String  statType      = "entropy";
    long    seed          = 0xae44a87f9edf1cbL;
    String  ignores;
    int     cloudFormationTimeout = 10; // wait for up to 10seconds
    int     verbose       = 0; // levels of verbosity
    int     exclusive     = 0; // exclusive split limit, 0 = exclusive split is disabled
    String  rng           = RNGKind.DETERMINISTIC.name();
  }

  static final OptArgs ARGS = new OptArgs();

  public static Map<Integer,Integer> parseStrata(String s){
    if(s.isEmpty())return null;
    String [] strs = s.split(",");
    Map<Integer,Integer> res = new HashMap<Integer, Integer>();
    for(String x:strs){
      String [] arr = x.split(":");
      res.put(Integer.parseInt(arr[0].trim()), Integer.parseInt(arr[1].trim()));
    }
    return res;
  }

  public static void main(String[] args) throws Exception {
    Arguments arguments = new Arguments(args);
    arguments.extract(ARGS);
    String[] h2oArgs;
    if(ARGS.h2oArgs == null) { // By default run using local IP, C.f. JUnitRunner
      File flat = Utils.writeFile("127.0.0.1:54327");
      h2oArgs = new String[] { "-ip=127.0.0.1", "-flatfile=" + flat.getAbsolutePath() };
    } else {
      if(ARGS.h2oArgs.startsWith("\"") && ARGS.h2oArgs.endsWith("\""))
        ARGS.h2oArgs = ARGS.h2oArgs.substring(1, ARGS.h2oArgs.length()-1);
      ARGS.h2oArgs = ARGS.h2oArgs.trim();
      h2oArgs = ARGS.h2oArgs.split("[ \t]+");
    }
    H2O.main(h2oArgs);
    ValueArray va;
    // get the input data
    if(ARGS.parsedKey != null) // data already parsed
      va = DKV.get(Key.make(ARGS.parsedKey)).get();
    else if(ARGS.rawKey != null) // data loaded in K/V, not parsed yet
      va = TestUtil.parse_test_key(Key.make(ARGS.rawKey),Key.make(TestUtil.getHexKeyFromRawKey(ARGS.rawKey)));
    else { // data outside of H2O, load and parse
      File f = new File(ARGS.file);
      Log.debug(Sys.RANDF,"Loading file ", f);
      Key fk = TestUtil.load_test_file(f);
      va = TestUtil.parse_test_key(fk,Key.make(TestUtil.getHexKeyFromFile(f)));
      DKV.remove(fk);
    }
    if(ARGS.ntrees == 0) {
      Log.warn(Sys.RANDF,"Nothing to do as ntrees == 0");
      UDPRebooted.T.shutdown.broadcast();
      return;
    }
    StatType st = ARGS.statType.equals("gini") ? StatType.GINI : StatType.ENTROPY;

    Map<Integer,Integer> strata = (ARGS.stratify && ARGS.strata != null) ? parseStrata(ARGS.strata) : null;

    double[] classWeights = null;
    if(ARGS.stratify && ARGS.strata != null) {
      Map<Integer,Integer> weights = parseStrata(ARGS.weights);
      int[] ks = new int[weights.size()];
      int i=0; for (Object clss : weights.keySet().toArray()) ks[i++]= (Integer)clss;
      Arrays.sort(ks);
      classWeights = new double[ks.length];
      i=0; for(int k : ks) classWeights[i++] = weights.get(k);
    }

    // Setup desired random generator.
    Utils.setUsedRNGKind(RNGKind.value(ARGS.rng));

    final int num_cols = va._cols.length;
    final int classcol = ARGS.classcol == -1 ? num_cols-1: ARGS.classcol; // Defaults to last column

    // Build the set of positive included columns
    BitSet bs = new BitSet();
    bs.set(0,va._cols.length);
    bs.clear(classcol);
    if (ARGS.ignores!=null)
      for( String s : ARGS.ignores.split(",") )
        bs.clear(Integer.parseInt(s));
    int cols[] = new int[bs.cardinality()+1];
    int idx=0;
    for( int i=bs.nextSetBit(0); i >= 0; i=bs.nextSetBit(i+1))
      cols[idx++] = i;
    cols[idx++] = classcol;     // Class column last
    assert idx==cols.length;
    assert ARGS.sample >0 && ARGS.sample<=100;
    assert ARGS.ntrees >=0;
    assert ARGS.binLimit > 0 && ARGS.binLimit <= Short.MAX_VALUE;

    Log.debug(Sys.RANDF,"Arguments used:\n"+ARGS.toString());
    final Key modelKey = Key.make("model");
    DRFJob drfJob = DRF.execute(modelKey,
                          cols,
                          va,
                          ARGS.ntrees,
                          ARGS.depth,
                          ARGS.binLimit,
                          st,
                          ARGS.seed,
                          ARGS.parallel==1,
                          classWeights,
                          ARGS.features, // number of split features or -1 (default)
                          ARGS.stratify ? Sampling.Strategy.STRATIFIED_LOCAL : Sampling.Strategy.RANDOM,
                          (ARGS.sample/100.0f),
                          /* FIXME strata*/ null,
                          ARGS.verbose,
                          ARGS.exclusive,
                          false, null);
    RFModel model = drfJob.get();  // block on all nodes!
    Log.debug(Sys.RANDF,"Random forest finished in TODO"/*+ drf._t_main*/);

    Timer t_valid = new Timer();
    // Get training key.
    Key valKey = model._dataKey;
    if(ARGS.outOfBagError && !ARGS.stratify){
      Log.debug(Sys.RANDF,"Computing out of bag error");
      CMFinal cm = ConfusionTask.make( model, valKey, classcol, null, true).get(); // block until CM is computed
      cm.report();
    }
    // Run validation.
    if(ARGS.validationFile != null && !ARGS.validationFile.isEmpty()){ // validate on the supplied file
      File f = new File(ARGS.validationFile);
      Log.debug(Sys.RANDF,"Loading validation file ",f);
      Key fk = TestUtil.load_test_file(f);
      ValueArray v = TestUtil.parse_test_key(fk,Key.make(TestUtil.getHexKeyFromFile(f)));
      valKey = v._key;
      DKV.remove(fk);
      CMFinal cm = ConfusionTask.make( model, valKey, classcol, null, false).get();
      cm.report();
    }

    Log.debug(Sys.RANDF,"Validation done in: " + t_valid);
    UDPRebooted.T.shutdown.broadcast();
  }
}
