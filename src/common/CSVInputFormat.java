package common;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class CSVInputFormat extends FileInputFormat<LongWritable, HashMap<String, String>> {
	@Override
	public RecordReader<LongWritable, HashMap<String, String>> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
		return new CSVRecordReader();
	}
	
	@Override
	protected boolean isSplitable(JobContext context, Path filename) {
		return false;
	}
}
