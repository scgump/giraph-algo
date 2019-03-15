package com.sunday.giraph.combiner;

import com.sunday.giraph.algo.sp.ShortestPathsWritable;
import java.util.LinkedList;
import org.apache.giraph.combiner.MessageCombiner;
import org.apache.hadoop.io.LongWritable;

/**
 * {@link MessageCombiner} that finds the minimum {@link ShortestPathsWritable}
 */
public class ShortestPathsMsgCombiner implements
    MessageCombiner<LongWritable, ShortestPathsWritable> {

  @Override
  public void combine(LongWritable vertexIndex, ShortestPathsWritable originalMessage,
      ShortestPathsWritable messageToCombine) {

    if (originalMessage.getDistance() > messageToCombine.getDistance()) {
      originalMessage.setDistance(messageToCombine.getDistance());
      originalMessage.setPaths(messageToCombine.getPaths());
    }
  }

  @Override
  public ShortestPathsWritable createInitialMessage() {
    return new ShortestPathsWritable(Integer.MAX_VALUE, new LinkedList<>());
  }
}
