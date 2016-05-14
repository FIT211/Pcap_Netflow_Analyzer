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
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import com.pangu.Netflow.Netflow_IO.*;

public class Transport_Analyzer {

	private static Configuration conf;
	private static String srcFileName;
	private static int reducer_num;
	private static Path inputDir;
	
	private static String database;
	private static String username;
	private static String password;
	
	public Transport_Analyzer() throws IOException{
		conf = new Configuration();
	}
	
	public Transport_Analyzer(Configuration Conf) throws IOException{
		conf = Conf;
		srcFileName = conf.getStrings("job.input.srcDir")[0];
		reducer_num = conf.getInt("job.reducer.number", 1);
		
		database = conf.get("job.mysql.database");
		username = conf.get("job.mysql.username");
		password = conf.get("job.mysql.password", "");
		//interval = conf.getInt("netflow.analyzer.interval", 60); 
	}
	
	public void start() throws ClassNotFoundException, InterruptedException{
	        
    	try{
    	   inputDir = new Path(srcFileName);
    		FileSystem fs = FileSystem.get(conf);
			Job job_state1 = get_JobConf("Netflow_Stat state1", inputDir);

			job_state1.waitForCompletion(true);
    	}catch (IOException e) {
    		// TODO Auto-generated catch block
    		e.printStackTrace();
    	}
	}
	
	private Job get_JobConf(String jobName, Path inFilePath) throws IOException{//获取第一阶段工作配置
		  
		DBConfiguration.configureDB(conf, "com.mysql.jdbc.Driver", database, username, password);
		
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(Netflow_Stat.class);
		
		job.setJobName(jobName);     
		job.setNumReduceTasks(reducer_num);  
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);	      
		
		job.setInputFormatClass(NetflowInputFormat.class);            
		//job.setOutputFormatClass(TextOutputFormat.class);  
		job.setInputFormatClass(NetflowInputFormat.class);          
		job.setOutputFormatClass(DBOutputFormat.class); 
		DBOutputFormat.setOutput(job, "Netflow_Transport", "router", "timestamp", "protocol", "flows", "packets", "bytes");
		
		job.setMapperClass(Map_Stats1.class);
		job.setCombinerClass(Combine_Stats1.class);          
		job.setReducerClass(Reduce_Stats1.class);    
		
		FileInputFormat.setInputPaths(job, inFilePath);
        
      return job;
	}
	
	public static class Map_Stats1 extends Mapper<LongWritable, BytesWritable, Text, Text>{
		private int interval = 60;
		private byte[] value_bytes;
		
		private int src_port;
		private int dst_port;
		private int duration;
		private int protocol;
		private long flow_size;
		private long packets;
		private long bytes;
		private long timestamp;
		private long PacketsSize;
		
		private Netflow_record record = new Netflow_record();
	   private Text text = new Text();
		private LongWritable longwrite = new LongWritable();
		
		@Override
		public void setup(Context context)throws IOException,InterruptedException{
			interval = context.getConfiguration().getInt("netflow.analyzer.interval", 60); 
			flow_size = 0;
			timestamp = 0;
		}
		
		@Override
		public void map(LongWritable key, BytesWritable value, Context context) throws IOException, InterruptedException {
			//System.out.println(flow_size++);
			//3327031
			value_bytes = value.getBytes();
			record.setNeflowRecord(value_bytes);
			
			timestamp = record.getStime();
			timestamp = timestamp - timestamp%300;
			
			protocol = get_protocol(record.getProto());
			
			packets = record.getPKT();
			bytes = record.getBytes();
			
		   context.write(new Text(timestamp + "\t"+protocol), new Text(packets+"\t"+bytes));

	    }//map`
	}//Map_States1
	
	public static class Combine_Stats1 extends Reducer<Text, Text, Text, Text> {	
		
		private String temp[];
		private long flows = 0;
		private long packets = 0;
		private long bytes = 0;
		
		@Override
		public void setup(Context context){
			System.out.println("Combine setup");
		}
		
		@Override
		public void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
			flows = 0;
			packets = 0;
			bytes = 0;

			while(value.iterator().hasNext()){
				temp = value.iterator().next().toString().split("\t");
				flows++;
				packets += Integer.parseInt(temp[0].trim());
				bytes += Integer.parseInt(temp[1].trim());
			}
			context.write(key, new Text(flows+"\t"+packets+"\t"+bytes));
			
		}
	    
	}
	
	public static class Reduce_Stats1 extends Reducer<Text, Text, NetflowDBWritable2, NetflowDBWritable2> {	
	
		private long flows;
		private long packets;
		private long bytes;
		private String temp[];
		private long timestamp;
		private int protocol;
		
		@Override
		public void setup(Context context){

		}
		
		@Override
		public void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
			packets = 0;
			bytes = 0;
			flows = 0;
			while(value.iterator().hasNext()){
				temp = value.iterator().next().toString().split("\t");
				flows += Integer.parseInt(temp[0].trim());
				packets += Integer.parseInt(temp[1].trim());
				bytes += Integer.parseInt(temp[2].trim());
			}
			
			temp = key.toString().split("\t");
			timestamp = Integer.parseInt(temp[0].trim());
			protocol = Integer.parseInt(temp[1].trim());
			
			context.write(new NetflowDBWritable2(1, (int)timestamp, (int)protocol, (int)flows, (int)packets, (int)bytes), null);
			  
		}
	    
	}


	public static int get_protocol(int protocol){
		switch(protocol){
		case 1:
			return 1;
		case 6:
			return 2;
		case 17:
			return 3;
		default:
			return 4;
		}
	}
	
}
