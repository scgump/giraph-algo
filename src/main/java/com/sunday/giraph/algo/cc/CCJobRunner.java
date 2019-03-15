package com.sunday.giraph.algo.cc;

import com.sunday.giraph.combiner.MinimumLongMsgCombiner;
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
 * MapReduce Job Entry for Connected Component
 */
public class CCJobRunner {
  private static final Logger logger = Logger.getLogger(CCJobRunner.class);

  public static void main(String[] args) throws Exception {

    GiraphConfiguration conf = new GiraphConfiguration();
    CommandLine cmd = ConfigurationUtil.parseArgs(conf, args);

    if (cmd == null) System.exit(-1);

    // op
    if (!cmd.hasOption("op")) {
      throw new IllegalArgumentException("Must set output path!");
    }
    String outPath = cmd.getOptionValue("op");
    logger.info("Parsed output path: " + outPath);

    // cc specified
    conf.setComputationClass(ConnComponentComputation.class);
    conf.setVertexInputFormatClass(LongLongNullTextInputFormat.class);
    conf.setMessageCombinerClass(MinimumLongMsgCombiner.class);
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
