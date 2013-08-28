package hex.rf;

import hex.rf.DIvotesTree.OOBEE;
import hex.rf.DRF.DRFParams;
import hex.rf.Sampling.Strategy;

import java.util.Arrays;
import java.util.Random;

import com.google.common.primitives.Ints;

import jsr166y.ForkJoinTask;
import water.*;
import water.util.*;
import water.util.Log.Tag.Sys;

/**
 * Random Forest based on iterative DIvotes algorithm.
 */
public class RFDIvotes {

  static final int[] NONE_ROWS = new int[] {};

  /** Build random forest for data stored on this node. */
  public static void build(
                      final Job job,
                      final DRFParams drfParams,
                      final Key  dataKey,
                      final Data data,
                      int ntrees,
                      int numSplitFeatures,
                      int[] rowsPerChunks,
                      Key[] localChunks) {
    assert drfParams._samplingStrategy == Strategy.RANDOM_WITH_REPLACEMENT : "Unsupported sampling strategy!";
    assert rowsPerChunks.length == localChunks.length;

    Timer  t_alltrees = new Timer();
    Log.debug(Sys.RANDF,"Building "+ntrees+" trees");
    Log.debug(Sys.RANDF,"Number of split features: "+ numSplitFeatures);
    Log.debug(Sys.RANDF,"Starting RF computation with "+ data.rows()+" rows ");

    // create sampler
    Random  rnd = Utils.getRNG(data.seed() + RandomForest.ROOT_SEED_ADD);
    byte producerId = (byte) H2O.SELF.index();
    Sampling sampler = RandomForest.createSampler(drfParams, rowsPerChunks);

    Key[] forest = new Key[ntrees];
    Key[] oobKeys = new Key[ntrees];
    int[] chunkRowsMapping = computeInitRows(dataKey, localChunks);
    int totalRows = Utils.sum(rowsPerChunks);

    double p = 0.75;
    double[] oobee = new double[ntrees];
    double e[] = new double[ntrees];
    double r[] = new double[ntrees];
    double c[] = new double[ntrees];
    int[] mispredRows = NONE_ROWS;
    // build each tree ~ k-th iteration
    for (int k = 0; k < ntrees; ++k) {
      long treeSeed = rnd.nextLong() + RandomForest.TREE_SEED_INIT; // make sure that enough bits is initialized

      // Generate k-th sample sampling with replacement |sample_k| = |data|
      int[] sample_k = Sampling.sample(data, treeSeed, 1, mispredRows, k>0?c[k-1]:1);
      // Save out-of-bag rows
      OOBSample oob_k = new OOBSample(k,data.complement(sample_k));
      oobKeys[k] = makeOOBKey(k);
      UKV.put(oobKeys[k], oob_k);
      System.err.println("Sample: " +sample_k.length +":"+ Arrays.toString(sample_k));
      System.err.println("OOB   : " +oob_k._oob.length+":"+ Arrays.toString(oob_k._oob));
      // Builder for a tree
      DIvotesTree treeBuilder = new DIvotesTree(job, data, producerId, drfParams._depth, drfParams._stat, numSplitFeatures, treeSeed,
                          k, drfParams._exclusiveSplitLimit, sampler, drfParams._verbose, sample_k);
      // Build a single tree
      ForkJoinTask.invokeAll(treeBuilder);
      forest[k] = treeBuilder._thisTreeKey;
      treeBuilder = null; // it is not needed anymore

      sample_k = null;
      // Vote about current forest
      OOBEE oobeeResult = DIvotesTree.voteOOB(k, dataKey, job.dest(), drfParams._classcol, localChunks, chunkRowsMapping, forest, oobKeys);
      mispredRows = oobeeResult._misrows;
      oobee[k] = oobeeResult.error();
      System.err.println("Forest 0.." + k + " has oobee="+oobee[k]);

      // Estimate e(k) of for all classifiers (currently k+1 trees are generated)
      r[k] = oobee[k]; // FIXME - should be error rate on testing dataset
      e[k] =  p* (k>0 ? e[k-1] : 0.5) + (1-p)*r[k];
      c[k] = e[k] / (1-e[k]);
      System.err.println("e["+k+"] = " + e[k]);
      System.err.println("c["+k+"] = " + c[k]);
    }

    System.err.println("e = " + Arrays.toString(e));
    System.err.println("c = " + Arrays.toString(c));
    System.err.println("r = " + Arrays.toString(r));

    Log.debug(Sys.RANDF,"All trees ("+ntrees+") done in "+ t_alltrees);
  }

  public static class OOBSample extends Iced {
    int[] _oob;
    int _k;
    public OOBSample(int k, int[] oob) { _k = k; _oob = oob; }
  }

  static Key makeOOBKey(int iteration) {
    int nodeIdx = H2O.SELF.index();
    Key k = Key.make("__TreeOOB__" + nodeIdx + "_" + iteration);
    return k;
  }

  static int[] computeInitRows(Key dataKey, Key[] localChunks) {
    ValueArray data = UKV.get(dataKey);
    long l = ValueArray.getChunkIndex(localChunks[localChunks.length-1])+1;
    int[] r = new int[Ints.checkedCast(l)];
    int off=0;
    for( Key k : localChunks ) {
      assert k.home();
      l = ValueArray.getChunkIndex(k);
      r[(int)l] = off;
      off += data.rpc(l);
    }
    return r;
  }

}
