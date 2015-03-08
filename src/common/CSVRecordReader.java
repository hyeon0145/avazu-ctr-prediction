package common;

import java.io.IOException;
import java.util.HashMap;
import java.util.Vector;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

public class CSVRecordReader extends RecordReader<LongWritable, HashMap<String, String>> {
	private LineRecordReader lineRecordReader;
	
	private boolean didReadHeader;
	private Vector<String> header;
	private HashMap<String, String> currentRecord;
	
	public CSVRecordReader() {
		this.lineRecordReader = new LineRecordReader();
		this.didReadHeader = false;
	}
	
	@Override
	public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
		this.lineRecordReader.initialize(split, context);
	}
	
	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		if (!didReadHeader) {
			if (!this.readHeader()) {
				return false;
			}
		}
		
		return this.readRecord();
	}

	@Override
	public LongWritable getCurrentKey() throws IOException, InterruptedException {
		return this.lineRecordReader.getCurrentKey();
	}

	@Override
	public HashMap<String, String> getCurrentValue() throws IOException, InterruptedException {
		return this.currentRecord;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return this.lineRecordReader.getProgress();
	}
	
	@Override
	public void close() throws IOException {
		this.lineRecordReader.close();
	}
	
	private boolean readHeader() throws IOException {
		if (!this.lineRecordReader.nextKeyValue()) {
			return false;
		}

		String[] items = this.lineRecordReader.getCurrentValue().toString().split(",");
		
		this.header = new Vector<String>();
		for (String item : items) {
			this.header.add(item.trim());
		}
		
		this.didReadHeader = true;
		return true;
	}
	
	private boolean readRecord() throws IOException {
		if (!this.lineRecordReader.nextKeyValue()) {
			return false;
		}
		
		String line = this.lineRecordReader.getCurrentValue().toString().trim();
		if (line.isEmpty()) {
			return false;
		}
		
		String[] items = line.split(",");
		
		this.currentRecord = new HashMap<String, String>();
		for (int i = 0; i < items.length; ++i) {
			this.currentRecord.put(this.header.get(i), items[i]);
		}
		
		return true;
	}
}