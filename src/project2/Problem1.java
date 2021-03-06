package project2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
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
	static String param = null;

	public static class Problem1Mapper extends Mapper<Object, Text, IntWritable, Text> {
		private Text word = new Text();
		static float x1, x2, y1, y2;

		protected void setup(Context context) {
			if (param != null) {
				String[] op = param.split(",");
				x1 = Float.parseFloat(op[0]);
				y1 = Float.parseFloat(op[1]);
				x2 = Float.parseFloat(op[2]);
				y2 = Float.parseFloat(op[3]);
				float temp;
				if (x1 > x2) {
					temp = x2;
					x2 = x1;
					x1 = temp;
				}
				if (y1 > y2) {
					temp = y2;
					y2 = y1;
					y1 = temp;
				}
			}
		}

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			int start, end = 0;
			while (itr.hasMoreTokens()) {
				word.set(itr.nextToken());
				String[] s = word.toString().split(",");
				if (s.length == 2) {
					if (param != null) {
						Float px = Float.parseFloat(s[0]);
						Float py = Float.parseFloat(s[1]);
						if (px < x1 || px > x2 || py < y1 || py > y2)
							continue;
					}
					context.write(new IntWritable(Math.round(Float.parseFloat(s[0]))), word);
				}
				if (s.length == 5) {
					if (param != null) {
						float rx = Float.parseFloat(s[1]);
						float ry = Float.parseFloat(s[2]);
						float rx2 = rx + Float.parseFloat(s[3]);
						float ry2 = ry - Float.parseFloat(s[4]);
						if (rx > x2 || rx2 < x1 || ry < y1 || ry2 > y2)
							continue;
					}
					start = Math.round(Float.parseFloat(s[1]));
					end = Math.round(Float.parseFloat(s[1]) + Float.parseFloat(s[4]));
					for (int i = start; i <= end; i++) {
						context.write(new IntWritable(i), word);
					}
				}
			}
		}
	}

	public static class Problem1Reducer extends Reducer<IntWritable, Text, Text, Text> {

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
					context.write(new Text(String.format("%-8s", r[0])), new Text("(" + point + ")"));
				}
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		if (args.length != 2 && args.length != 3) {
			System.err.println("Usage: Problem-1 <HDFS input file> <HDFS output file> <optional-W(x1,y1,x2,y2)>");
			System.exit(2);
		}
		Job job = new Job(conf, "Problem-1");
		job.setJarByClass(Problem1.class);
		job.setMapperClass(Problem1Mapper.class);
		job.setReducerClass(Problem1Reducer.class);
		//job.setNumReduceTasks(2);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		if (args.length == 3)
			param = args[2];
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}