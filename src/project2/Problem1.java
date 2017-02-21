package project2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Problem1 {

	public static class Problem1Mapper extends Mapper<Object, Text, IntWritable, Text> {
		private Text word = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			int start, end = 0;
			while (itr.hasMoreTokens()) {
				word.set(itr.nextToken());
				String[] s = word.toString().split(",");
				if (s.length == 2) {
					context.write(new IntWritable((int) Float.parseFloat(s[0])), word);
				}
				if (s.length == 5) {
					start = (int) Float.parseFloat(s[1]);
					end = (int) Math.ceil(Float.parseFloat(s[1]) + Float.parseFloat(s[4]));
					for (int i = start; i <= end; i++) {
						context.write(new IntWritable(i), word);
					}
				}
			}
		}
	}

	public static class Problem1Reducer extends Reducer<IntWritable, Text, Text, Text> {

		private static HashMap<String, String> result = new HashMap<String, String>();
		Comparator<String> pc = new Comparator<String>() {
			@Override
			public int compare(String s1, String s2) {
				String[] s1s = s1.split(",");
				String[] s2s = s2.split(",");
				Float y1 = Float.parseFloat(s1s[1]);
				Float y2 = Float.parseFloat(s2s[1]);
				return y2.compareTo(y1);
			}
		};
		Comparator<String> rc = new Comparator<String>() {
			@Override
			public int compare(String s1, String s2) {
				String[] s1s = s1.split(",");
				String[] s2s = s2.split(",");
				Float y1 = Float.parseFloat(s1s[2]);
				Float y2 = Float.parseFloat(s2s[2]);
				Float h1 = Float.parseFloat(s1s[4]);
				Float h2 = Float.parseFloat(s2s[4]);
				if (y1.equals(y2))
					return h2.compareTo(h1);
				else
					return y2.compareTo(y1);
			}
		};

		public void reduce(IntWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			ArrayList<String> rects = new ArrayList<String>();
			ArrayList<String> points = new ArrayList<String>();

			// Initiate rects and points
			for (Text value : values) {
				String[] s = value.toString().split(",");
				if (s.length == 2) {
					points.add(value.toString());
				}
				if (s.length == 5) {
					rects.add(value.toString());
				}
			}

			Collections.sort(points, pc);
			Collections.sort(rects, rc);

			// Join points with rects
			int start = 0;
			for (String rect : rects) {
				String[] r = rect.split(",");
				float ry = Float.parseFloat(r[2]);
				float ry2 = ry - Float.parseFloat(r[4]);
				float rx = Float.parseFloat(r[1]);
				float rx2 = rx + Float.parseFloat(r[3]);
				for (int i = start; i < points.size(); i++) {
					String point = points.get(i);
					String[] p = point.split(",");
					float px = Float.parseFloat(p[0]);
					float py = Float.parseFloat(p[1]);
					if (py > ry) {
						start = i;
						continue;
					}
					if (py < ry2) {
						break;
					}
					if (px < rx || px > rx2)
						continue;
					String temp = result.get(r[0]);
					if (temp != null)
						result.put(r[0], temp + ",(" + point + ")");
					else
						result.put(r[0], "(" + point + ")");		
				}
			}
/*
			for (String point : points) {
				String[] p = point.split(",");
				float px = Float.parseFloat(p[0]);
				float py = Float.parseFloat(p[1]);

				for (String rect : rects) {
					String[] r = rect.split(",");
					float ry = Float.parseFloat(r[2]);
					if (py > ry)
						continue;
					float ry2 = ry - Float.parseFloat(r[3]);
					if (py < ry2)
						continue;
					float rx = Float.parseFloat(r[1]);
					if (px < rx)
						continue;
					float rx2 = rx + Float.parseFloat(r[4]);
					if (px <= rx2) {
						String temp = result.get(r[0]);
						if (temp != null)
							result.put(r[0], temp + ",(" + point + ")");
						else
							result.put(r[0], "(" + point + ")");
					}
				}
			}
*/
		}

		protected void cleanup(Context context) throws IOException, InterruptedException {
			Set<String> keyResult = result.keySet();
			for (String key : keyResult) {
				context.write(new Text(key), new Text(result.get(key)));
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		if (args.length != 2) {
			System.err.println("Usage: Problem-1 <HDFS input file> <HDFS output file>");
			System.exit(2);
		}
		Job job = new Job(conf, "Problem-1");
		job.setJarByClass(Problem1.class);
		job.setMapperClass(Problem1Mapper.class);
		job.setReducerClass(Problem1Reducer.class);
		job.setNumReduceTasks(2);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
