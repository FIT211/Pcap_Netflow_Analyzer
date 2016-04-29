package com.pangu.Netflow.Analyzer;


import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import com.pangu.Netflow.Netflow_IO.*;

public class Netflow_Stat {
	
	private static Configuration conf;
	private static String srcFileName;
	private static String dstFileName;
	private static int reducer_num;
	private static Path outputDir;
	private static Path inputDir;
	
	public Netflow_Stat() throws IOException{
		conf = new Configuration();
	}
	
	public Netflow_Stat(Configuration Conf) throws IOException{
		conf = Conf;
		srcFileName = conf.getStrings("pcap.record.srcDir")[0];
		dstFileName = conf.getStrings("pcap.record.dstDir")[0]+"/Netflow_Stat";
		reducer_num = conf.getInt("pcap.record.reducer.num", 1);
		//interval = conf.getInt("pcap.record.interval", 60); 
	}
	
	public void start() throws ClassNotFoundException, InterruptedException{
        
    	try{

    	   outputDir = new Path(dstFileName + "/state1/");
    	   inputDir = new Path(srcFileName);
    		FileSystem fs = FileSystem.get(conf);
			Job job_state1 = get_state1_JobConf("Netflow_Stat state1", inputDir, outputDir);  
			
			// delete any output that might exist from a previous run of this job
			if (fs.exists(FileOutputFormat.getOutputPath(job_state1))) {
				fs.delete(FileOutputFormat.getOutputPath(job_state1), true);
	        }

			job_state1.waitForCompletion(true);
			/*
			
			outputDir = new Path(dstFileName + "/state2/");
			inputDir = FileOutputFormat.getOutputPath(job_state1);
			Job job_state2 = get_state2_JobConf("application analyzer state2", inputDir, outputDir); 
			
			// delete any output that might exist from a previous run of this job
			if (fs.exists(FileOutputFormat.getOutputPath(job_state2))) {
				fs.delete(FileOutputFormat.getOutputPath(job_state2), true);
	        }
			
	      job_state2.waitForCompletion(true);
	   
	        */
        }catch (IOException e) {
    		// TODO Auto-generated catch block
    		e.printStackTrace();
    	}
    }
	
	private Job get_state1_JobConf(String jobName, Path inFilePath, Path outFilePath) throws IOException{//获取第一阶段工作配置
		  
		Job job = Job.getInstance(conf);
		job.setJarByClass(Netflow_Stat.class);
		job.setJobName(jobName);     
		job.setNumReduceTasks(reducer_num);       
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);	       
		job.setInputFormatClass(NetflowInputFormat.class);          
		job.setOutputFormatClass(TextOutputFormat.class);     
		job.setMapperClass(Map_Stats1.class);
		job.setCombinerClass(Combine_Stats1.class);          
		job.setReducerClass(Reduce_Stats1.class);    
		
		FileInputFormat.setInputPaths(job, inFilePath);
		FileOutputFormat.setOutputPath(job, outFilePath);
        
      return job;
	}

	private Job get_state2_JobConf(String jobName, Path inFilePath, Path outFilePath) throws IOException{//获取第二阶段工作配置

		Job job = Job.getInstance(conf);
		job.setJarByClass(Netflow_Stat.class);
		
		job.setJobName(jobName); 
		job.setNumReduceTasks(reducer_num);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);	
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setMapperClass(Map_Stats2.class);
		job.setCombinerClass(Reduce_Stats2.class);
		job.setReducerClass(Reduce_Stats2.class);    
        
		FileInputFormat.setInputPaths(job, inFilePath);
		FileOutputFormat.setOutputPath(job, outFilePath);
        
      return job;
	} 
	
	
	public static class Map_Stats1 extends Mapper<LongWritable, BytesWritable, Text, LongWritable>{
		private int interval = 60;
		private byte[] value_bytes;
		
		private int src_port;
		private int dst_port;
		private int duration;
		private int protocol;
		private long flow_size;
		
		private Netflow_record record = new Netflow_record();
	   private Text text = new Text();
		private LongWritable longwrite = new LongWritable();
		
		@Override
		public void setup(Context context)throws IOException,InterruptedException{
			interval = context.getConfiguration().getInt("pcap.record.interval", 60); 
			flow_size = 0;
		}
		
		@Override
		public void map(LongWritable key, BytesWritable value, Context context) throws IOException, InterruptedException {
			//System.out.println(flow_size++);
			//3327031
			value_bytes = value.getBytes();
			record.setNeflowRecord(value_bytes);
			/*	
			src_port = record.getSrcPort();
			//text.clear();Finished spill 15
			//text.set("sp" + "\t"+src_port);
			context.write(new Text("sp" + "\t"+src_port), new LongWritable(1));
			
			dst_port = record.getDstPort();
			//text.clear();
			//text.set("dp" + "\t"+dst_port);
		   context.write(new Text("dp" + "\t"+dst_port), new LongWritable(1));
		  */
			/*
			duration = record.getEtime()-record.getStime();
			if(0 < duration && duration < 100)
				duration = 0;
			else if(100 <= duration && duration < 500)
				duration = 1;
			else if(500 <= duration && duration < 1000)
				duration = 2;
			else if(1000 <= duration && duration < 5000)
				duration = 3;
			else if(5000 <= duration && duration < 10000)
				duration = 4;
			else if(10000 <= duration && duration < 20000)
				duration = 5;
			else if(20000 <= duration && duration < 50000)
				duration = 6;
			else
				duration = 7;
			//text.clear();
			//text.set("du" + "\t"+duration);
		   context.write(new Text("du" + "\t"+duration), new LongWritable(1));
		    */
			protocol = record.getProto();
			//text.clear();
			//text.set("pr" + "\t"+protocol);
		   context.write(new Text("pr" + "\t"+protocol), new LongWritable(1));
		   /*
			flow_size = record.getBytes();
			if(0 < flow_size && flow_size < 100)
				flow_size = 0;
			else if(0 <= flow_size && flow_size < 200)
				flow_size = 1;
			else if(200 <= flow_size && flow_size < 300)
				flow_size = 2;
			else if(300 <= flow_size && flow_size < 500)
				flow_size = 3;
			else if(500 <= flow_size && flow_size < 1000)
				flow_size = 5;
			else if(1000 < flow_size && flow_size < 2000)
				flow_size = 10;
			else if(2000 <= flow_size && flow_size < 5000)
				flow_size = 20;
			else
				flow_size = 50;
			text.clear();
			text.set("si" + "\t"+flow_size);
		   context.write(text, longwrite);
			*/
	    }//map`
	}//Map_States1
	
	public static class Combine_Stats1 extends Reducer<Text, LongWritable, Text, LongWritable> {	
		
		private long count = 0;
		
		@Override
		public void setup(Context context){
			System.out.println("Combine setup");
		}
		
		@Override
		public void reduce(Text key, Iterable<LongWritable> value, Context context) throws IOException, InterruptedException {

			count = 0;
			while(value.iterator().hasNext()){
				value.iterator().next();
				count ++;
			}
			context.write(key, new LongWritable(count));
			  
		}
	    
	}
	
	public static class Reduce_Stats1 extends Reducer<Text, LongWritable, Text, LongWritable> {	
	
		private  long sum = 0;
		private long count = 0;
		private String temp[];
		
		private MultipleOutputs mos;
		@Override
		public void setup(Context context){
			mos = new MultipleOutputs(context);
			
		}
		@Override
		public void reduce(Text key, Iterable<LongWritable> value, Context context) throws IOException, InterruptedException {
			sum = 0;
			count = 0;
			while(value.iterator().hasNext()){
				sum += value.iterator().next().get();
			}
			
			//mos.write(temp[0], key, new LongWritable(count));
			context.write(key, new LongWritable(sum));
			  
		}
	    
	}


	public static class Map_Stats2 extends Mapper<LongWritable, Text, Text, LongWritable>{
		String[] substring;
		String[] s;
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
			substring = value.toString().split("\t");
			context.write(new Text(substring[0]+"\t"+substring[2]), new LongWritable(Long.parseLong(substring[3])));
			
		}
		
	}
	
    public static class Reduce_Stats2 extends Reducer<Text, LongWritable, Text, LongWritable> {
    	private long sum;

    	@Override
		public void reduce(Text key, Iterable<LongWritable> value, Context context) throws IOException, InterruptedException {
	
	      sum = 0;

	       while(value.iterator().hasNext()) 		 				
	    	   sum += value.iterator().next().get();
	       context.write(key, new LongWritable(sum));        

    	}
	
    }
}