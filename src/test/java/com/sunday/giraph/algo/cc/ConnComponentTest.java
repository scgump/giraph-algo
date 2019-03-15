package com.sunday.giraph.algo.cc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.base.Splitter;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.SetMultimap;
import com.sunday.giraph.combiner.MinimumLongMsgCombiner;
import java.util.Set;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.edge.ByteArrayEdges;
import org.apache.giraph.io.formats.IdWithValueTextOutputFormat;
import org.apache.giraph.io.formats.LongLongNullTextInputFormat;
import org.apache.giraph.utils.InternalVertexRunner;
import org.junit.jupiter.api.Test;

/**
 * Test {@link ConnComponentComputation}
 */
public class ConnComponentTest {

  /**
   * test connected component in memory
   */
  @Test
  public void simpleConnectedComponent() throws Exception {
    GiraphConfiguration conf = new GiraphConfiguration();

    conf.setVertexInputFormatClass(LongLongNullTextInputFormat.class);
    conf.setComputationClass(ConnComponentComputation.class);
    conf.setOutEdgesClass(ByteArrayEdges.class);
    conf.setMessageCombinerClass(MinimumLongMsgCombiner.class);
    conf.setVertexOutputFormatClass(IdWithValueTextOutputFormat.class);

    // run internally
    Iterable<String> results = InternalVertexRunner.run(conf, testGraph);

    SetMultimap<Integer,Integer> components = parseResults(results);

    Set<Integer> componentIDs = components.keySet();
    assertEquals(3, componentIDs.size());
    assertTrue(componentIDs.contains(1));
    assertTrue(componentIDs.contains(6));
    assertTrue(componentIDs.contains(9));

    Set<Integer> componentOne = components.get(1);
    assertEquals(7, componentOne.size());
    assertTrue(componentOne.contains(1));
    assertTrue(componentOne.contains(2));
    assertTrue(componentOne.contains(3));
    assertTrue(componentOne.contains(4));
    assertTrue(componentOne.contains(5));
    assertTrue(componentOne.contains(12));
    assertTrue(componentOne.contains(13));

    Set<Integer> componentTwo = components.get(6);
    assertEquals(5, componentTwo.size());
    assertTrue(componentTwo.contains(6));
    assertTrue(componentTwo.contains(7));
    assertTrue(componentTwo.contains(8));
    assertTrue(componentTwo.contains(10));
    assertTrue(componentTwo.contains(11));

    Set<Integer> componentThree = components.get(9);
    assertEquals(1, componentThree.size());
    assertTrue(componentThree.contains(9));
  }

  private SetMultimap<Integer,Integer> parseResults(
      Iterable<String> results) {
    SetMultimap<Integer,Integer> components = HashMultimap.create();
    for (String result : results) {
      Iterable<String> parts = Splitter.on('\t').split(result);
      int vertex = Integer.parseInt(Iterables.get(parts, 0));
      int component = Integer.parseInt(Iterables.get(parts, 1));
      components.put(component, vertex);
    }
    return components;
  }

  // test graph
  private final String[] testGraph = new String[] {
      "1 2 3",
      "2 1 4 5",
      "3 1 4",
      "4 2 3 5 13",
      "5 2 4 12 13",
      "12 5 13",
      "13 4 5 12",

      "6 7 8",
      "7 6 10 11",
      "8 6 10",
      "10 7 8 11",
      "11 7 10",

      "9" };
}
