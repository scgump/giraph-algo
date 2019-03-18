package com.sunday.giraph.algo.sp;

import static com.sunday.giraph.algo.sp.SingleSourceSPComputation.SOURCE_VERTEX_ID;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import com.sunday.giraph.combiner.ShortestPathsMsgCombiner;
import java.util.HashMap;
import java.util.Map;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.edge.ByteArrayEdges;
import org.apache.giraph.io.formats.IdWithValueTextOutputFormat;
import org.apache.giraph.utils.InternalVertexRunner;
import org.junit.jupiter.api.Test;

/**
 * Test {@link SingleSourceSPComputation}
 */
public class SingleSourceSPTest {

  /**
   * test single source shortest path in memory
   */
  @Test
  public void simpleSSSP() throws Exception {
    GiraphConfiguration conf = new GiraphConfiguration();

    conf.setVertexInputFormatClass(LongSPNullTextInputFormat.class);
    conf.setComputationClass(SingleSourceSPComputation.class);
    conf.setMessageCombinerClass(ShortestPathsMsgCombiner.class);
    conf.setOutEdgesClass(ByteArrayEdges.class);
    conf.setVertexOutputFormatClass(IdWithValueTextOutputFormat.class);

//    SOURCE_VERTEX_ID.set(conf, 1L);
    conf.set(SOURCE_VERTEX_ID.getKey(), "1");

    // run internally
    Iterable<String> results = InternalVertexRunner.run(conf, testGraph);

    Map<Integer, Tuple> paths = parseResults(results);

    assertTrue(paths.containsKey(1));
    assertEquals(paths.get(1).dist, 0);
    assertEquals(paths.get(1).paths, null);

    assertTrue(paths.containsKey(2));
    assertEquals(paths.get(2).dist, 1);
    assertEquals(paths.get(2).paths, "1");

    assertTrue(paths.containsKey(3));
    assertEquals(paths.get(3).dist, 1);
    assertEquals(paths.get(3).paths, "1");


    assertTrue(paths.containsKey(4));
    assertEquals(paths.get(4).dist, 2);
    assertEquals(paths.get(4).paths, "1,3");


    assertTrue(paths.containsKey(5));
    assertEquals(paths.get(5).dist, 2);
    assertEquals(paths.get(5).paths, "1,2");

  }

  private Map<Integer, Tuple> parseResults(Iterable<String> results) {
    Map<Integer, Tuple> map = new HashMap<>();

    int vertex, dist;
    String paths;
    for (String result : results) {
      Iterable<String> parts = Splitter.on('\t').split(result);
      vertex = Integer.parseInt(Iterables.get(parts, 0));
      dist = Integer.parseInt(Iterables.get(parts, 1));
      if (dist != 0) {
        paths = Iterables.get(parts, 2);
      } else {
        paths = null;
      }
      map.put(vertex, new Tuple(dist, paths));
    }

    return map;
  }


  private class Tuple {
    int dist;
    String paths;

    public Tuple(int dist, String paths) {
      this.dist = dist;
      this.paths = paths;
    }
  }

  // test graph
  private final String[] testGraph = new String[] {
      "1 2 3",
      "2 5",
      "3 4",
      "4 5",
      "5" };
}
