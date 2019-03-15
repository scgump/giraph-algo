package com.sunday.giraph.algo.sp;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import org.apache.giraph.conf.LongConfOption;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;

/**
 * A simple algo that computes the shortest from a single source
 */
public class SingleSourceSPComputation extends
    BasicComputation<LongWritable, ShortestPathsWritable, NullWritable, ShortestPathsWritable> {

  // source vertex id
  public static final LongConfOption SOURCE_VERTEX_ID =
      new LongConfOption("single.source.shortest.path.source.id", -1L,
          "The source vertex id used in shortest path");


  /**
   * Is this vertex the source
   */
  private boolean isSource(Vertex<LongWritable, ?, ?> vertex) {
    return vertex.getId().get() == SOURCE_VERTEX_ID.get(getConf());
  }

  @Override
  public void compute(Vertex<LongWritable, ShortestPathsWritable, NullWritable> vertex,
      Iterable<ShortestPathsWritable> messages) throws IOException {

    // init vertex value
    if (getSuperstep() == 0) {
      vertex.setValue(new ShortestPathsWritable(Integer.MAX_VALUE, new LinkedList<>()));
    }

    // process income msgs
    List<Long> minPaths = vertex.getValue().getPaths();
    int minDist = isSource(vertex) ? 0 : Integer.MAX_VALUE;
    for (ShortestPathsWritable msg : messages) {
      if (msg.getDistance() < minDist) {
        minDist = msg.getDistance();
        minPaths = msg.getPaths();
      }
    }

    //
    if (minDist < vertex.getValue().getDistance()) {
      // update vertex
      vertex.getValue().setDistance(minDist);
      vertex.getValue().setPaths(minPaths);

      // send
      int outDist = minDist + 1;
      List<Long> outPaths = new LinkedList<>(minPaths);
      outPaths.add(vertex.getId().get());
      ShortestPathsWritable msg = new ShortestPathsWritable(outDist, outPaths);

      for (Edge<LongWritable, NullWritable> edge : vertex.getEdges()) {
        sendMessage(edge.getTargetVertexId(), msg);
      }
    }

    vertex.voteToHalt();
  }
}
