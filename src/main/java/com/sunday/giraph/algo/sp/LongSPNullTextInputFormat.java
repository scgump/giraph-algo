package com.sunday.giraph.algo.sp;

import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Pattern;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.io.formats.TextVertexInputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * Parse vertex input for {@link SingleSourceSPComputation}
 */
public class LongSPNullTextInputFormat extends
    TextVertexInputFormat<LongWritable, ShortestPathsWritable, NullWritable> {

  /** Separator of the vertex and neighbors */
  private static final Pattern SEPARATOR = Pattern.compile("[\t ]");

  @Override
  public TextVertexReader createVertexReader(InputSplit split, TaskAttemptContext context)
      throws IOException {
    return new LongSPNullVertexReader();
  }

  /**
   * Vertex Reader
   */
  public class LongSPNullVertexReader extends TextVertexReaderFromEachLineProcessed<String[]> {
    /** Cached vertex id for the current line */
    private LongWritable id;

    @Override
    protected String[] preprocessLine(Text line) throws IOException {
      String[] tokens = SEPARATOR.split(line.toString());
      id = new LongWritable(Long.parseLong(tokens[0]));
      return tokens;
    }

    @Override
    protected LongWritable getId(String[] line) throws IOException {
      return id;
    }

    @Override
    protected ShortestPathsWritable getValue(String[] line) throws IOException {
      return new ShortestPathsWritable(Integer.MAX_VALUE, new LinkedList<>());
    }

    @Override
    protected Iterable<Edge<LongWritable, NullWritable>> getEdges(String[] line)
        throws IOException {
      List<Edge<LongWritable, NullWritable>> edges =
          Lists.newArrayListWithCapacity(line.length - 1);

      for (int n = 1; n < line.length; n++) {
        edges.add(EdgeFactory.create(new LongWritable(Long.parseLong(line[n]))));
      }
      return edges;
    }
  }
}
