package hex.gbm;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.util.Arrays;

import org.junit.BeforeClass;
import org.junit.Test;

import water.*;
import water.parser.*;
import water.fvec.*;

public class GBMTest extends TestUtil {

  @BeforeClass public static void stall() { stall_till_cloudsize(1); }

  // ==========================================================================
  /*@Test*/ public void testBasicGBM() {
    File file = TestUtil.find_test_file("./smalldata/logreg/prostate.csv");
    Key fkey = NFSFileVec.make(file);
    Frame fr = ParseDataset2.parse(Key.make("prostate.hex"),new Key[]{fkey});
    UKV.remove(fkey);
    try {
      assertEquals(380,fr._vecs[0].length());

      // Prostate: predict on CAPSULE which is in column #1; move it to last column
      UKV.remove(fr.remove("ID")._key);   // Remove patient ID vector
      Vec capsule = fr.remove("CAPSULE"); // Remove capsule
      fr.add("CAPSULE",capsule);          // Move it to the end

      GBM gbm = GBM.start(GBM.makeKey(),fr,11);
      gbm.get();                  // Block for result
      UKV.remove(gbm._dest);
    } finally {
      UKV.remove(fr._key);
    }
  }

  /*@Test*/ public void testCovtypeGBM() {
    File file = TestUtil.find_test_file("../datasets/UCI/UCI-large/covtype/covtype.data");
    Key fkey = NFSFileVec.make(file);
    Frame fr = ParseDataset2.parse(Key.make("cov1.hex"),new Key[]{fkey});
    UKV.remove(fkey);
    System.out.println("Parsed into "+fr);
    for( int i=0; i<fr._vecs.length; i++ )
      System.out.println("Vec "+i+" = "+fr._vecs[i]);

    Key rkey = load_test_file(file,"covtype.data");
    Key vkey = Key.make("cov2.hex");
    ParseDataset.parse(vkey, new Key[]{rkey});
    UKV.remove(rkey);
    ValueArray ary = UKV.get(vkey);
    System.out.println("Parsed into "+ary);

    try {
      assertEquals(581012,fr._vecs[0].length());

      // Covtype: predict on last column
      GBM gbm = GBM.start(GBM.makeKey(),fr,10);
      gbm.get();                  // Block for result
      UKV.remove(gbm._dest);
    } finally {
      UKV.remove(fr ._key);
      UKV.remove(ary._key);
    }
  }

}
