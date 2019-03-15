package com.sunday.giraph.algo.sp;

import static com.sunday.giraph.algo.sp.SingleSourceSPComputation.SOURCE_VERTEX_ID;

import com.sunday.giraph.algo.cc.CCJobRunner;
import com.sunday.giraph.combiner.ShortestPathsMsgCombiner;
import com.sunday.giraph.util.ConfigurationUtil;
import org.apache.commons.cli.CommandLine;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.edge.ByteArrayEdges;
import org.apache.giraph.io.formats.IdWithValueTextOutputFormat;
import org.apache.giraph.io.formats.LongLongNullTextInputFormat;
import org.apache.giraph.job.GiraphJob;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

/**
 * MapReduce Job Entry for Single Source Shortest Paths
 */
public class SSSPJobRunner {
  private static final Logger logger = Logger.getLogger(SSSPJobRunner.class);

  public static void main(String[] args) throws Exception {

    GiraphConfiguration conf = new GiraphConfiguration();
    CommandLine cmd = ConfigurationUtil.parseArgs(conf, args);

    if (cmd == null) System.exit(-1);

    // check source id
    if (SOURCE_VERTEX_ID.isDefaultValue(conf)) {
      throw new IllegalArgumentException("Must set source vertex id!");
    }

    // op
    if (!cmd.hasOption("op")) {
      throw new IllegalArgumentException("Must set output path!");
    }
    String outPath = cmd.getOptionValue("op");
    logger.info("Parsed output path: " + outPath);

    // sssp specified
    conf.setVertexInputFormatClass(LongLongNullTextInputFormat.class);
    conf.setComputationClass(SingleSourceSPComputation.class);
    conf.setMessageCombinerClass(ShortestPathsMsgCombiner.class);
    conf.setOutEdgesClass(ByteArrayEdges.class);
    conf.setVertexOutputFormatClass(IdWithValueTextOutputFormat.class);

    // for MR2, since this prop is deprecated
    conf.set("mapred.job.tracker", "non-local");

    GiraphJob job = new GiraphJob(conf, CCJobRunner.class.getSimpleName());
    FileOutputFormat.setOutputPath(job.getInternalJob(), new Path(outPath));

    if (job.run(true)) {
      logger.info("Ended well");
    } else {
      logger.info("Ended with Failure");
    }
  }
}
