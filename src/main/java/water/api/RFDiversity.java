package water.api;

import hex.rf.*;
import water.H2O;
import water.ValueArray;

import com.google.gson.*;

/**
 * Page for random forest scoring.
 *
 * Redirect directly to RF view.
 */
public class RFDiversity extends Request {

  protected final H2OHexKey          _dataKey  = new H2OHexKey(DATA_KEY);
  protected final RFModelKey         _modelKey = new RFModelKey(MODEL_KEY);
  protected final HexKeyClassCol     _classCol = new HexKeyClassCol(CLASS, _dataKey);
  protected final Int                _numTrees = new NTree(NUM_TREES, _modelKey);
  protected final Bool               _clearCM  = new Bool(JSON_CLEAR_CM, false, "Clear cache of model confusion matrices");

  public static final String JSON_CLEAR_CM      = "clear_confusion_matrix";

  RFDiversity() {
    _numTrees._readOnly = true;
  }

  @Override protected Response serve() {
    RFModel model = _modelKey.value();
    ValueArray data = _dataKey.value();
    int[] chunkRowsMapping = RFDIvotes.computeInitRows(data._key);
    int[][][] diversity = DIvotesTree.diversity(model._selfKey, data._key, _classCol.value(), chunkRowsMapping);
    int nodes = H2O.CLOUD.size();
    JsonObject response = new JsonObject();
    response.addProperty(NODES, nodes);
    JsonArray matrix = new JsonArray();
    for (int i=0; i<nodes;i++) {
      JsonArray row = new JsonArray();
      for (int j=0; j<nodes; j++) {
        int idx =  i*nodes + j;
        row.add(toJson(diversity[idx]));
      }
      matrix.add(row);
    }
    response.add(DIVERSITY, matrix);
    Response r = Response.done(response);
    r.setBuilder(DIVERSITY, new DiversityMatrixBuilder());
    return r;
  }

  static final JsonObject toJson(int[][] diversity) {
    JsonObject r = new JsonObject();
    if (diversity!=null) {
      r.addProperty("a", diversity[0][0]);
      r.addProperty("b", diversity[0][1]);
      r.addProperty("c", diversity[1][0]);
      r.addProperty("d", diversity[1][1]);
    }
    return r;
  }

  public static class DiversityMatrixBuilder extends ObjectBuilder {

    @Override public String build(Response response, JsonElement element, String contextName) {
      StringBuilder sb = new StringBuilder();
      sb.append("<h3>Diversity matrix</h3>");
      sb.append("<table>");
      JsonArray matrix = (JsonArray) element;
      for (JsonElement e : matrix) {
        JsonArray row = (JsonArray) e;
        sb.append("<tr>");
        for (JsonElement o : row) {
          sb.append("<td>");
          toHtml(sb,o.getAsJsonObject());
          sb.append("</td>");
        }
        sb.append("</tr>");
      }
      sb.append("</table>");

      return sb.toString();
    }

    static void toHtml(StringBuilder sb, JsonObject o) {
      sb.append("<table border='1'>");
      sb.append("<tr>");
      sb.append("<td>").append(o.get("a")).append("</td>");
      sb.append("<td>").append(o.get("b")).append("</td>");
      sb.append("</tr>");
      sb.append("<tr>");
      sb.append("<td>").append(o.get("c")).append("</td>");
      sb.append("<td>").append(o.get("d")).append("</td>");
      sb.append("</tr>");
      sb.append("</table>");
    }
  }
}
