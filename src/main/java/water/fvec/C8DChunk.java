package water.fvec;

import water.*;

// The empty-compression function, where data is in 'int's.
public class C8DChunk extends Chunk {
  C8DChunk( byte[] bs ) { _mem=bs; _start = -1; _len = _mem.length>>3; }
  @Override protected final long   at8_impl( int i ) {
    double res = UDP.get8d(_mem,i<<3);
    return Double.isNaN(res)?_vec._iNA:(long)res;
  }
  @Override protected final double atd_impl( int i ) {
    double res = UDP.get8d(_mem,i<<3);
    return Double.isNaN(res)?_vec._fNA:res;
  }
  @Override boolean set8_impl(int idx, long l) { return false; }
  @Override boolean set8_impl(int i, double d) {
    UDP.set8d(_mem,i<<3,d);
    return true;
  }
  @Override boolean hasFloat() { return true; }
  @Override public AutoBuffer write(AutoBuffer bb) {return bb.putA1(_mem,_mem.length);}
  @Override public C8DChunk read(AutoBuffer bb) {
    _mem = bb.bufClose();
    _start = -1;
    _len = _mem.length>>3;
    assert _mem.length == _len<<3;
    return this;
  }
  @Override NewChunk inflate_impl(NewChunk nc) { throw H2O.fail(); }
}
