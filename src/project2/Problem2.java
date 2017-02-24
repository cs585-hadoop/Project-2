package project2;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Problem2 {
	public static class Problem2Mapper extends Mapper<Object, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			Pattern pattern = Pattern.compile("\"Flags.*?,");
			Matcher matcher=pattern.matcher(value.toString());
			if(matcher.find()){
				String flagNum=value.toString().substring(matcher.start(),matcher.end()-1);
				String[] s= flagNum.split(":");
				s[1]=s[1].trim();
				context.write(new Text(s[1]), one);
			}
		}
	}

	public static class Problem2Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {

		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			context.write(key, new IntWritable(sum));
		}
		@Override
		protected void cleanup(Reducer<Text, IntWritable, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(new Path(args[1]))) {
			fs.delete(new Path(args[1]), true);
		}

		if (args.length != 2) {
			System.err.println("Usage: Problem-2 <HDFS input file> <HDFS output file> ");
			System.exit(2);
		}
		Job job = new Job(conf, "Problem-2");
		job.setJarByClass(Problem2.class);
		job.setMapperClass(Problem2Mapper.class);
		job.setCombinerClass(Problem2Reducer.class);
		job.setReducerClass(Problem2Reducer.class);
		// job.setNumReduceTasks(2);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setInputFormatClass(JsonInputFormat.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
