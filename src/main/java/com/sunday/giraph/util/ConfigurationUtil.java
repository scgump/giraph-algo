package com.sunday.giraph.util;

import java.io.IOException;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.io.formats.GiraphFileInputFormat;
import org.apache.giraph.utils.ConfigurationUtils;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

/**
 * Translate command line args into Configuration Key-Value pairs.
 */
public class ConfigurationUtil {
  private ConfigurationUtil() {}

  private static Logger logger = Logger.getLogger(ConfigurationUtil.class);

  // options
  private static Options OPTIONS;

  static {
    OPTIONS = new Options();
    OPTIONS.addOption("h", false, "Help");
    OPTIONS.addOption("w", true, "Worker number");
    OPTIONS.addOption("vip", true, "Vertex input path");
    OPTIONS.addOption("op", true, "Output path");
    OPTIONS.addOption("ca", true, "provide custom  arguments for the job configuration "
        + "in the form: -ca <param1>=<value1> -ca <param2>=<value2> etc. "
        + "It can appear multiple times, and the last one has effect for the same param.");
  }

  /**
   * Translate arguments into Configuration Key-Value pairs.
   * Output path should be set in job object
   */
  public static CommandLine parseArgs(final GiraphConfiguration conf, final String[] args)
      throws ParseException, IOException {
    // verify we have args at all (can't run without them!)
    if (args.length == 0) {
      throw new IllegalArgumentException("No arguments were provided (try -h)");
    }

    CommandLineParser parser = new BasicParser();
    CommandLine cmd = parser.parse(OPTIONS, args);

    // simply printing help or info, return normally but kill job run
    if (cmd.hasOption("h")) {
      printHelp();
      return null;
    }

    if (cmd.hasOption("w")) {
      int numOfWorkers = Integer.parseInt(cmd.getOptionValue("w"));
      logger.info("Parsed number of workers: " + numOfWorkers);
      conf.setWorkerConfiguration(numOfWorkers, numOfWorkers, 100.0f);
    }

    if (cmd.hasOption("vip")) {
      String vip = cmd.getOptionValue("vip");
      logger.info("Parsed vertex input path: " + vip);
      GiraphFileInputFormat.addVertexInputPath(conf, new Path(vip));
    }

    if (cmd.hasOption("ca")) {
      for (String caOpt : cmd.getOptionValues("ca")) {
        String[] fields = caOpt.split("=");
        if (fields.length != 2) {
          throw new IllegalArgumentException("Invalid custom arguments: " + caOpt);
        }
        logger.info("Parsed custom argument, key: " + fields[0] + ", value: " + fields[1]);
        conf.set(fields[0], fields[1]);
      }
    }

    return cmd;
  }

  /**
   * Utility to print CLI help messsage for registered options.
   */
  private static void printHelp() {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp(ConfigurationUtils.class.getName(), OPTIONS, true);
  }

}
