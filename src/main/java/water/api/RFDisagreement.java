package water.api;

import hex.rf.*;
import hex.rf.DIvotesTree.Disagreement;
import water.H2O;
import water.ValueArray;

import com.google.gson.*;

/**
 * Page for measuring nodes disagreement on local data per chunk.
 */
public class RFDisagreement extends Request {

  protected final H2OHexKey          _dataKey  = new H2OHexKey(DATA_KEY);
  protected final RFModelKey         _modelKey = new RFModelKey(MODEL_KEY);
  protected final HexKeyClassCol     _classCol = new HexKeyClassCol(CLASS, _dataKey);
  protected final Int                _numTrees = new NTree(NUM_TREES, _modelKey);

  RFDisagreement() {
    _numTrees._readOnly = true;
  }

  @Override protected Response serve() {
    RFModel model = _modelKey.value();
    ValueArray data = _dataKey.value();
    Disagreement disagreement = DIvotesTree.disagreement(model._selfKey, data._key, _classCol.value());
    JsonObject response = new JsonObject();
    response.add("DISAGREEMENT", toJson(disagreement));
    Response r = Response.done(response);
    r.setBuilder("DISAGREEMENT", new DisagreementMatrixBuilder());
    return r;
  }

  static final JsonObject toJson(Disagreement dis) {
    JsonObject r = new JsonObject();
    r.add("CHUNK_HOMES", toJson(dis._chunkHomes));
    r.add("NODE_ERR_PER_CHUNK", toJson(dis._nodeErrPerChunk));
    r.addProperty(NODES, dis._nodes);
    r.add("TREES_PER_NODE", toJson(dis._treesPerNode));
    r.add("LINES_PER_CHUNK", toJson(dis._linesPerChunk));
    return r;
  }

  static final JsonArray toJson(int[] arr) {
    JsonArray r = new JsonArray();
    for (int i : arr) {
      r.add(new JsonPrimitive(i));
    }
    return r;
  }

  static final JsonArray toJson(int[][] aa) {
    JsonArray r = new JsonArray();
    for (int[] a : aa) {
      r.add(toJson(a));
    }
    return r;
  }

  public static class DisagreementMatrixBuilder extends ObjectBuilder {

    @Override public String build(Response response, JsonObject jobj, String contextName) {

      StringBuilder sb = new StringBuilder();
      sb.append("<h3>Disagreement matrix</h3>");
      sb.append("<table class='table table-striped table-bordered table-condensed'>");
      JsonArray chunksHomes = jobj.get("CHUNK_HOMES").getAsJsonArray();
      JsonArray matrix = jobj.get("NODE_ERR_PER_CHUNK").getAsJsonArray();
      JsonArray treesPerNode = jobj.get("TREES_PER_NODE").getAsJsonArray();
      JsonArray linesPerChunk = jobj.get("LINES_PER_CHUNK").getAsJsonArray();
      int nodes = jobj.get(NODES).getAsInt();
      sb.append("<tr><th>Chunks \\ Nodes</th>");
      for (int i=0;i<nodes;i++) { sb.append("<th>").append(i).append(" (").append(treesPerNode.get(i).getAsInt()).append(")</th>"); }
      sb.append("</tr>");
      // per chunks
      int cIdx = 0;
      for (JsonElement e : matrix) {
        JsonArray row = (JsonArray) e;
        sb.append("<tr><th>").append(cIdx).append(" (").append(linesPerChunk.get(cIdx).getAsInt()).append(")</th>");
        // per nodes
        int nodIdx = 0;
        for (JsonElement o : row) {
          if (chunksHomes.get(cIdx).getAsInt()==nodIdx) sb.append("<td style='background-color:LightGreen'>");
          else sb.append("<td>");
          sb.append(o.getAsNumber());
          sb.append("</td>");
          nodIdx++;
        }
        cIdx++;
        sb.append("</tr>");
      }
      sb.append("</table>");

      return sb.toString();
    }
  }
}
