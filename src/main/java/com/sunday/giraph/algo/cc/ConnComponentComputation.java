package com.sunday.giraph.algo.cc;

import java.io.IOException;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;

/**
 * A simple algo that computes the connected component
 */
public class ConnComponentComputation extends
    BasicComputation<LongWritable, LongWritable, NullWritable, LongWritable> {

  /**
   * Propagates the smallest vertex id to all neighbors. Will always choose to
   * halt and only reactivate if a smaller id has been sent to it.
   *
   * @param vertex Vertex
   * @param messages Iterator of messages from the previous superstep.
   * @throws IOException
   */
  @Override
  public void compute(Vertex<LongWritable, LongWritable, NullWritable> vertex,
      Iterable<LongWritable> messages) throws IOException {

    long currMin = vertex.getValue().get();

    // First superstep is special, because we can simply look at the neighbors
    if (getSuperstep() == 0) {
      long neighborId;
      for (Edge<LongWritable, NullWritable> edge : vertex.getEdges()) {
        neighborId = edge.getTargetVertexId().get();
        if (neighborId < currMin) {
          currMin = neighborId;
        }
      }

      // only need to send value if it is not the own id
      if (currMin != vertex.getValue().get()) {
        vertex.setValue(new LongWritable(currMin));
        LongWritable neighbor;
        for (Edge<LongWritable, NullWritable> edge : vertex.getEdges()) {
          neighbor = edge.getTargetVertexId();
          if (neighbor.get() > currMin) {
            sendMessage(neighbor, vertex.getValue());
          }
        }
      }

      vertex.voteToHalt();
      return;
    }

    boolean changed = false;
    // did we get a smaller id?
    long candidateMin;
    for (LongWritable msg : messages) {
      candidateMin = msg.get();
      if (candidateMin < currMin) {
        currMin = candidateMin;
        changed = true;
      }
    }

    if (changed) {
      vertex.setValue(new LongWritable(currMin));
      sendMessageToAllEdges(vertex, vertex.getValue());
    }

    vertex.voteToHalt();
  }
}
