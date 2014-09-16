package com.ismartv.statistics.conversions;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class TextPairKeyPartitioner extends Partitioner<TextPair, Text> {

	public int getPartition(TextPair key, Text value, int numPartitions) {
		return (key.getFirst().hashCode() & Integer.MAX_VALUE) % numPartitions;
	}
}