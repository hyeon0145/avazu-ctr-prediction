package trainer;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import common.CSVInputFormat;

public class Trainer {
	public static class FeatureHashingMapper extends Mapper<LongWritable, HashMap<String, String>, IntWritable, Example> {
		private int numberOfBins;
		private int numberOfReducers;
		
		@Override
		protected void setup(Mapper<LongWritable, HashMap<String, String>, IntWritable, Example>.Context context) throws IOException, InterruptedException {
			Configuration configuration = context.getConfiguration();
			this.numberOfBins = configuration.getInt("numberOfBins", 0);
			this.numberOfReducers = configuration.getInt("numberOfReducers", 0);
		}
		
		@Override
		protected void map(LongWritable key, HashMap<String, String> value, Mapper<LongWritable, HashMap<String, String>, IntWritable, Example>.Context context) throws IOException, InterruptedException {
			Example example = new Example();
			example.getX().put(0, 1); // default x_0
			example.setY(Integer.parseInt(value.get("click")));

			value.remove("id");
			value.remove("click");
			
			value.put("hour", value.get("hour").substring(6)); // use  only *real hour*
			
			// applying feature hashing
			for (Entry<String, String> entry : value.entrySet()) {
				int hash = (entry.getKey() + entry.getValue()).hashCode();
				int index = hash % this.numberOfBins; // 0 <= index < this.numberOfBins
				if (index < 0) {
					index += this.numberOfBins;
				}
				index += 1; // 1 <= index < this.numberOfBins + 1
				
				if (example.getX().containsKey(index)) {
					example.getX().put(index, example.getX().get(index) + 1);
				} else {
					example.getX().put(index, 1);
				}
			}
			
			context.write(new IntWritable(this.pickGroup()), example);
		}
		
		private int pickGroup() {
			return new Random().nextInt(this.numberOfReducers);
		}
	}
	
	public static class StochasticGradientDescentReducer extends Reducer<IntWritable, Example, NullWritable, Text> {
		private int numberOfBins;
		private double learningRate;
		
		private double[] theta;
		
		@Override
		protected void setup(Reducer<IntWritable, Example, NullWritable, Text>.Context context) throws IOException, InterruptedException {
			Configuration configuration = context.getConfiguration();
			this.numberOfBins = configuration.getInt("numberOfBins", 0);
			this.learningRate = (double)configuration.getFloat("learningRate", 0.1f);
			
			this.theta = new double[this.numberOfBins + 1]; // + 1 for x_0
		}
		
		@Override
		protected void reduce(IntWritable key, Iterable<Example> values, Reducer<IntWritable, Example, NullWritable, Text>.Context context) throws IOException, InterruptedException {
			for (Example example : values) {
				this.doStochasticGradientDescent(example);
			}
		}
		
		@Override
		protected void cleanup(Reducer<IntWritable, Example, NullWritable, Text>.Context context) throws IOException, InterruptedException {
			StringBuilder builder = new StringBuilder();
			builder.append(theta[0]);
			for (int i = 1; i < theta.length; ++i) {
				builder.append('\t');
				builder.append(theta[i]);
			}
			
			context.write(NullWritable.get(), new Text(builder.toString()));
		}
		 
		private void doStochasticGradientDescent(Example example) {
			double h = this.h(example);
			
			for (Entry<Integer, Integer> entry : example.getX().entrySet()) {
				double gradient = (h - example.getY()) * entry.getValue();
				theta[entry.getKey()] -= this.learningRate * gradient;
			}
		}
		
		private double h(Example example) {
			double xTheta = 0.0;
			for (Entry<Integer, Integer> entry : example.getX().entrySet()) {
				xTheta += entry.getValue() * theta[entry.getKey()];
			}
			
			return this.g(xTheta);
		}
		
		private double g(double z) {
			// to prevent overflow -> -50 <= z <= 50
			z = Math.max(z, -50.0);
			z = Math.min(z, 50.0);
			
			return 1.0 / (1.0 + Math.exp(-z)); 
		}
	}
	
	public static void main(String[] arguments) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration configuration = new Configuration();
		String[] remainingArguments = new GenericOptionsParser(configuration, arguments).getRemainingArgs();
		if (remainingArguments.length != 5) {
			System.err.println("Usage: train <number of bins> <learning rate> <number of reducer> <data input> <theta output>");
			System.exit(1);
		}

		int numberOfBins = Integer.parseInt(remainingArguments[0]);
		float learningRate = Float.parseFloat(remainingArguments[1]);
		int numberOfReducers = Integer.parseInt(remainingArguments[2]);
		Path inputPath = new Path(remainingArguments[3]);
		Path outputPath = new Path(remainingArguments[4]);
		
		configuration.setInt("numberOfBins", numberOfBins);
		configuration.setFloat("learningRate", learningRate);
		configuration.setInt("numberOfReducers", numberOfReducers);
		
		Job job = new Job(configuration, "train");
		job.setJarByClass(Trainer.class);

		job.setMapperClass(FeatureHashingMapper.class);
		job.setReducerClass(StochasticGradientDescentReducer.class);

		job.setInputFormatClass(CSVInputFormat.class);
		
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Example.class);
		
		job.setNumReduceTasks(numberOfReducers);

		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);
		
		boolean result = job.waitForCompletion(true);
		if (result) {
			Trainer.mergeTheta(configuration, outputPath);
		}
		
		System.exit(result ? 0 : 1);
	}
	
	private static void mergeTheta(Configuration configuration, Path outputPath) throws IOException {
		FileSystem fileSystem = outputPath.getFileSystem(configuration);
		int numberOfBins = configuration.getInt("numberOfBins", 0);
		int numberOfReducers = configuration.getInt("numberOfReducers", 0);
		
		double[] theta = new double[numberOfBins + 1];
		for (int i = 0; i < theta.length; ++i) {
			theta[i] = 0.0;
		}
		
		for (int i = 0; i < numberOfReducers; ++i) {
			Path partialThetaPath = new Path(String.format("%s/part-r-%05d", outputPath.toString(), i));
			InputStream stream = fileSystem.open(partialThetaPath);
			BufferedReader reader = new BufferedReader(new InputStreamReader(stream));
			
			String line = reader.readLine().trim();
			String[] items = line.split("\t");
			for (int j = 0; j < items.length; ++j) {
				theta[j] += Double.parseDouble(items[j]);
			}
			
			reader.close();
			stream.close();
		}
	
		Path thetaPath = new Path(outputPath + "/theta");
		OutputStream stream = fileSystem.create(thetaPath);
		BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(stream));
		
		StringBuilder builder = new StringBuilder();
		builder.append(theta[0] / numberOfReducers);
		for (int i = 1; i < theta.length; ++i) {
			builder.append('\t');
			builder.append(theta[i] / numberOfReducers);
		}
		
		writer.write(builder.toString());
		
		writer.close();
		stream.close();
	}
}
