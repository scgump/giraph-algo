package com.sunday.giraph.algo.sp;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import org.apache.hadoop.io.Writable;

/**
 * Storage the min distance, and paths
 */
public class ShortestPathsWritable implements Writable {
  /** Default output delimiter */
  private static final String fieldsDel = "\t";
  private static final String pathsDel = ",";


  // shortest distance
  private int distance;

  // vertex ids from source to target
  private List<Long> paths;

  public ShortestPathsWritable() {
  }

  public ShortestPathsWritable(int distance, List<Long> paths) {
    this.distance = distance;
    this.paths = paths;
  }

  public int getDistance() {
    return distance;
  }

  public void setDistance(int distance) {
    this.distance = distance;
  }

  public List<Long> getPaths() {
    return paths;
  }

  public void setPaths(List<Long> paths) {
    this.paths = paths;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    Objects.nonNull(distance);
    Objects.nonNull(paths);

    out.writeInt(distance);

    out.writeInt(paths.size());
    for (long vid : paths) {
      out.writeLong(vid);
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    distance = in.readInt();

    int len = in.readInt();
    paths = new LinkedList<>();
    for (int i = 0; i < len; i++) {
      paths.add(in.readLong());
    }
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();

    sb.append(distance).append(fieldsDel);
    boolean isFirst = true;

    for (long vid : paths) {
      if (isFirst) {
        sb.append(vid);
        isFirst = false;
      } else {
        sb.append(pathsDel);
        sb.append(vid);
      }
    }

    return sb.toString();
  }
}
