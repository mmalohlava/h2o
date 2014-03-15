package hex;

import hex.rng.MersenneTwisterRNG;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.*;

import water.util.Log;
import water.util.Utils;

/**
 * Looks for parameters on a set of objects and perform random search.
 */
class ParamsSearch {
  @Retention(RetentionPolicy.RUNTIME)
  public @interface Info {
    /**
     * Parameter search will move the value relative to origin.
     */
    double origin() default 0;
  }

  Param[] _params;
  Random _rand = new MersenneTwisterRNG(new Random().nextLong());
  double _rate = .1;
  boolean _booleans;

  class Param {
    int _objectIndex;
    Field _field;
    double _initial, _best, _last;

    void assertSupported() {
      Class t = _field.getType();
      assert t == boolean.class || t == float.class || t == int.class;
    }

    void modify(Object o) throws Exception {
      if( _field.getType() == boolean.class ) {
        if( _rand.nextDouble() < _rate ) {
          _last = _best == 0 ? 1 : 0;
          _field.set(o, _last == 1);
        }
      } else {
        double delta = _best * _rate;
        double min = _best - delta, max = _best + delta;
        _last = min + _rand.nextDouble() * (max - min);
        if( _field.getType() == float.class )
          _field.set(o, (float) _last);
        else if( _field.getType() == int.class )
          _field.set(o, (int) _last);
      }
      String change = _best + " -> " + _last;
      Log.info(this + ": " + change);
    }

    void write() {
      Log.info(this + ": " + _best);
    }

    String objectName() {
      return _field.getDeclaringClass().getName() + " " + _objectIndex;
    }

    @Override public String toString() {
      return objectName() + "." + _field.getName();
    }
  }

  void run(Object... os) throws Exception {
    ArrayList<Object> expanded = new ArrayList<Object>();
    for( Object o : os ) {
      if( o instanceof Object[] )
        expanded.addAll(Arrays.asList((Object[]) o));
      else if( o instanceof Collection )
        expanded.addAll((Collection) o);
      else
        expanded.add(o);
    }

    if( _params == null ) {
      ArrayList<Param> params = new ArrayList<Param>();
      for( int i = 0; i < expanded.size(); i++ ) {
        Class c = expanded.get(i).getClass();
        ArrayList<Field> fields = new ArrayList<Field>();
        Utils.getAllFields(fields, c);
        for( Field f : fields ) {
          f.setAccessible(true);
          if( (f.getModifiers() & Modifier.STATIC) == 0 ) {
            Object v = f.get(expanded.get(i));
            if( v instanceof Number || (_booleans && v instanceof Boolean) ) {
              Param param = new Param();
              param._objectIndex = i;
              param._field = f;
              if( v instanceof Boolean )
                param._initial = ((Boolean) v).booleanValue() ? 1 : 0;
              else
                param._initial = ((Number) v).doubleValue();
              param._last = param._best = param._initial;
              params.add(param);
              param.write();
            }
          }
        }
      }
      _params = params.toArray(new Param[0]);
      Log.info(toString());
    } else {
      for( int i = 0; i < _params.length; i++ )
        modify(expanded, i);
    }
  }

  void modify(ArrayList<Object> expanded, int i) throws Exception {
    Object o = expanded.get(_params[i]._objectIndex);
    _params[i].modify(o);
  }

  void save() {
    for( int i = 0; i < _params.length; i++ )
      _params[i]._best = _params[i]._last;
  }

//  @Override public String toString() {
//    StringBuilder sb = new StringBuilder();
//    int objectIndex = -1;
//    for( Param param : _params ) {
//      if( objectIndex != param._objectIndex ) {
//        objectIndex = param._objectIndex;
//        sb.append(param._field.getDeclaringClass().getName() + " " + objectIndex + '\n');
//      }
//      sb.append("  " + param._field.getName() + ": " + param._best + '\n');
//    }
//    return sb.toString();
//  }
}