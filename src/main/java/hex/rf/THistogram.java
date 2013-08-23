package hex.rf;

import java.util.Arrays;

public class THistogram {
  int _lastIdx;
  float[] _values;
  int  [] _num;
  final float _epsilon;

  public THistogram() { this(256, (float) 0.01); }
  public THistogram(int initSize, float epsilo) {
    _values = new float[initSize];
    _num    = new int[initSize];
    _lastIdx = 0;
    _epsilon = epsilo;
  }

  int resize() {
    int newsize = 2*_values.length;
    _values = Arrays.copyOf(_values, newsize);
    _num    = Arrays.copyOf(_num, newsize);
    return newsize;
  }

  public void add(float v) {
    for (int i=0; i<_lastIdx; i++) {
      if (Math.abs(_values[i]-v) < _epsilon) {
        _num[i]++; return;
      }
    }
    _values[_lastIdx] = v;
    _num   [_lastIdx] = 1;
    _lastIdx++;
    if (_lastIdx == _num.length) resize();
  }

  @Override public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("  val  |  num\n");
    sb.append("----------------\n");
    for (int i=0; i<_lastIdx; i++)
    sb.append("  ").append(_values[i]).append("  |").append("  ").append(_num[i]).append('\n');
    return sb.toString();
  }
}
