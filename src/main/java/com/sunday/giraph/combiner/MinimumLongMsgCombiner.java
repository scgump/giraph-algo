package com.sunday.giraph.combiner;

import org.apache.giraph.combiner.MessageCombiner;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;

/**
 * {@link MessageCombiner} that finds the minimum {@link LongWritable}
 */
public class MinimumLongMsgCombiner implements MessageCombiner<WritableComparable, LongWritable> {

  @Override
  public void combine(WritableComparable vertexIndex, LongWritable originalMessage,
      LongWritable messageToCombine) {
    if (originalMessage.get() > messageToCombine.get()) {
      originalMessage.set(messageToCombine.get());
    }
  }

  @Override
  public LongWritable createInitialMessage() {
    return new LongWritable(Long.MAX_VALUE);
  }
}
