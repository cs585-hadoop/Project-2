package project2;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Problem3 {
	private static HashMap<Integer,Float[]> kseed=new HashMap<>();
	private static int counter=1;
	private static boolean flag_change=false;

	public static class Problem3Mapper extends Mapper<Object, Text, IntWritable, Text> {
		private Text word = new Text();
		private static BufferedReader reader;

		protected void setup(Context context)  throws IOException,InterruptedException,FileNotFoundException {
		if(counter==1){
			Path[] cacheFilesLocal = DistributedCache.getLocalCacheFiles(context.getConfiguration());
			for (Path eachPath : cacheFilesLocal) {
				
				if (eachPath.getName().toString().trim().equals("seed.txt")) {
					loadSeedHashMap(eachPath, context);
				}
			}
		}
		}	
		
		private void loadSeedHashMap(Path p, Context context) throws IOException,FileNotFoundException{
			reader=new BufferedReader(new FileReader(p.toString()));
			String inputline=reader.readLine();
			String[] input;
			int count=0;
			while(inputline!=null){
				input=inputline.split(",");
				kseed.put(count,new Float[]{Float.parseFloat(input[0]),Float.parseFloat(input[1])});	
				inputline=reader.readLine();
				count++;
			}
		}


		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			int closeIndex=0;
			while (itr.hasMoreTokens()) {
				
				word.set(itr.nextToken()+":1");
				String[] input=word.toString().split(",");
				Float x=Float.parseFloat(input[0]);
				Float y=Float.parseFloat(input[1]);
				Double min=Double.POSITIVE_INFINITY;
				
				for(Integer index:kseed.keySet()){
					
					float x0=kseed.get(index)[0];
					float y0=kseed.get(index)[1];
					double x1=Math.pow(Math.abs(x-x0),2);
					double y1=Math.pow(Math.abs(y-y0),2);
					double distance=Math.sqrt(x1+y1);
					
					if(distance< min){
						closeIndex=index;
						min=distance;
					}
				}
				context.write(new IntWritable(closeIndex),word);
			}
			
		}

}
	public static class Q2Combiner extends Reducer<IntWritable, Text, IntWritable, Text> {
		private Text result = new Text();

		public void reduce(IntWritable key, Iterable<Text> values, Context context) 
				throws IOException, InterruptedException {
			float sum_x=0,sum_y=0;
			int count=0;
			for(Text value:values){
				float x=Integer.parseInt(value.toString().split(",")[0]);
				float y=Integer.parseInt(value.toString().split(",")[1].split(":")[0]);
				sum_x+=x;
				sum_y+=y;
				count++;
			}
			context.write(key,new Text(sum_x+","+sum_y+":"+count));
			
		}
	}
	public static class Problem3Reducer extends Reducer<IntWritable, Text, IntWritable, Text> {

		

		public void reduce(IntWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			float mean_x=0,mean_y=0;
			float sum_x=0,sum_y=0;
			int count=0;
			
			for(Text value:values){
				float x=Integer.parseInt(value.toString().split(",")[0]);
				float y=Integer.parseInt(value.toString().split(",")[1].split(":")[0]);
				count+=Integer.parseInt(value.toString().split(",")[1].split(":")[1]);
				sum_x+=x;
				sum_y+=y;
			}
			
			mean_x=sum_x/count;
			mean_y=sum_y/count;
			
			Float[] seed=kseed.get(key.get());
			if(!(seed[0]==mean_x && seed[1]==mean_y)){
				kseed.put(key.get(), new Float[]{mean_x,mean_y});
				flag_change=true;	
			}
			

		}

		protected void cleanup(Context context) throws IOException, InterruptedException {
			if(!flag_change && counter==5){
				for(int index:kseed.keySet()){
					context.write(new IntWritable(index),new Text(kseed.get(index)[0]+","+kseed.get(index)[1]));
				}
			}
			
		}
	}

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		if (args.length != 3) {
			System.err.println("Usage: Query-4 <HDFS input file> <HDFS output file> <HDFS cache file>");
			System.exit(2);
		}
		conf.set("mapred.textoutputformat.separator", ",");
		// TODO:add cache files
		DistributedCache.addCacheFile(new Path(args[2]).toUri(), conf);
		
		Job job = new Job(conf, "k-means");
		job.setJarByClass(Problem3.class);
		job.setMapperClass(Problem3Mapper.class);
		job.setReducerClass(Problem3Reducer.class);
		job.setNumReduceTasks(2);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.waitForCompletion(true);
		
		while(flag_change && counter!=5){
			flag_change=false;
			job = new Job(conf, "k-means");
			job.setJarByClass(Problem3.class);
			job.setMapperClass(Problem3Mapper.class);
			job.setReducerClass(Problem3Reducer.class);
			job.setNumReduceTasks(2);
			job.setOutputKeyClass(IntWritable.class);
			job.setOutputValueClass(Text.class);
			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));
			job.waitForCompletion(true);
			counter++;
		}
		
		
	}
}
