package project2;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.StringTokenizer;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class Problem3v1{

	public static class Problem3v1Mapper extends Mapper<Object, Text, Text, Text> {
		private static BufferedReader reader;
		private static HashMap<Integer,Float[]> kseed=new HashMap<>();
		private static Text word=new Text();
		private static int hashcount=0;
        
		protected void setup(Context context)  throws IOException,InterruptedException,FileNotFoundException {
			int counter=context.getConfiguration().getInt("counter",0);
			Path[] cacheFilesLocal = DistributedCache.getLocalCacheFiles(context.getConfiguration());
			if(counter==1){
				
				for (Path eachPath : cacheFilesLocal) {
					if (eachPath.getName().toString().trim().equals("seeds.txt")) {
						loadSeedHashMap(eachPath, context,true);
					}
				}
			}
			else{
			
				for (Path eachPath : cacheFilesLocal) {
					System.out.println(eachPath.toString());
					if (eachPath.getName().startsWith("part")) {
						loadSeedHashMap(eachPath, context,false);
					}
				}	
			}

			}
		
		    
			
			private void loadSeedHashMap(Path p, Context context,boolean flag) throws IOException,FileNotFoundException{
				reader=new BufferedReader(new FileReader(p.toString()));
				String inputline=reader.readLine();
				String[] input;
				while(inputline!=null){
					input=inputline.split(",");
					kseed.put(hashcount,new Float[]{Float.parseFloat(input[0]),Float.parseFloat(input[1])});	
					inputline=reader.readLine();
					hashcount++;
				}
			}
			
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			
			StringTokenizer itr = new StringTokenizer(value.toString());
			int closeIndex=0;
			while (itr.hasMoreTokens()) {
				
				word.set(itr.nextToken());
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
				context.write(new  Text(kseed.get(closeIndex)[0]+","+kseed.get(closeIndex)[1]),new  Text(word.toString()+":1"));
			}
			
		}
		
		}

	public static class Problem3v1Reducer extends Reducer<Text, Text, Text, IntWritable> {
		boolean flag;
		private static float delta=(float) 0.001;
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			float sum_x=0,sum_y=0;
			int count=0;
			float mean_x=0,mean_y=0;
			
			for(Text value:values){
				
				float x = Float.parseFloat(value.toString().split(",")[0]);
				float y = Float.parseFloat(value.toString().split(",")[1].split(":")[0]);
				count += Integer.parseInt(value.toString().split(",")[1].split(":")[1]);
				sum_x+=x;
				sum_y+=y;
				
			}
			
			mean_x=sum_x/count;
			mean_y=sum_y/count;
			
			String[] seed=key.toString().split(",");
	
			System.out.println(seed[0]+","+seed[1]+"::"+mean_x+","+mean_y);

			if(!((Math.abs( Float.parseFloat(seed[0])-mean_x)<delta) && (Math.abs(Float.parseFloat(seed[1])-mean_y)<delta))){

				context.write(new Text(mean_x+","+mean_y), new IntWritable(1));
				
			}
			else
			{
				context.write(new Text(mean_x+","+mean_y), new IntWritable(0));
			}
		
		}
	}

	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		int counter=1;
		boolean flag=false;
		
		conf.setInt("counter", counter);

		if (args.length != 3) {
			System.err.println("Usage: Problem-3 <HDFS input file> <HDFS output file> <seed file> ");
			System.exit(2);
		}
		
		Path inputPath = new Path(args[0]);
        Path basePath = new Path(args[1] + "_iterations");
        
		if (fs.exists(new Path(args[1]))) {
			fs.delete(new Path(args[1]), true);
		}

		if (args.length != 3) {
			System.err.println("Usage: Problem-3 <HDFS input file> <HDFS output file> <cache file> ");
			System.exit(2);
		}
		
		DistributedCache.addCacheFile(new Path(args[2]).toUri(), conf);
		conf.set("mapred.textoutputformat.separator", ",");
		Job job = new Job(conf, "iterations_"+counter);
		job.setJarByClass(Problem3v1.class);
		job.setMapperClass(Problem3v1Mapper.class);
		//job.setCombinerClass(Problem3v1Reducer.class);
		job.setReducerClass(Problem3v1Reducer.class);
	    job.setNumReduceTasks(2);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, new Path(basePath,counter+""));
		job.waitForCompletion(true);
		
		//check the reduce output
		Path outputPath=new Path(basePath,counter+"");
		conf = new Configuration();
		flag=checkOutput(fs, outputPath,conf);
		counter++;
		
		while(counter<=5 && !flag){
		    
			conf.setInt("counter", counter);
			conf.set("mapred.textoutputformat.separator", ",");
			outputPath=new Path(basePath,counter+"");
			job = new Job(conf, "iterations_"+counter);
			job.setJarByClass(Problem3v1.class);
			job.setMapperClass(Problem3v1Mapper.class);
			//job.setCombinerClass(Problem3v1Reducer.class);
			job.setReducerClass(Problem3v1Reducer.class);
		    job.setNumReduceTasks(2);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);
			FileInputFormat.addInputPath(job, inputPath);
			FileOutputFormat.setOutputPath(job, outputPath);
			job.waitForCompletion(true);
			conf = new Configuration();
			flag=checkOutput(fs, outputPath,conf); 
			counter++;
		}

	}
	public static boolean checkOutput(FileSystem fs,Path  p,Configuration conf) throws IOException{
		FileStatus[] status = fs.listStatus(p);
		boolean flag=false;
		System.out.println(status.length);
		for (int i=0;i<status.length;i++){
			
			if(status[i].getPath().getName().startsWith("part")){
				DistributedCache.addCacheFile(status[i].getPath().toUri(), conf);
				BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(status[i].getPath())));
				String line;
				String[] input;
		        line=br.readLine();
		        while (line != null){
		                input=line.split(",");
		                if(input[2]=="1"){
		                	flag=true;
		                	break;
		                }
		                line=br.readLine();
		        }
			} 
		}	
		return flag;
	}
		
}
	


