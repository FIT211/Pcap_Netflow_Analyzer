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
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.pangu.Netflow.Netflow_IO.*;

public class Netflow_Analyzer_DB {

	private static Configuration conf;
	private static String srcFileName;
	private static String dstFileName;
	private static int reducer_num;
	private static Path outputDir;
	private static Path inputDir;
	
	public Netflow_Analyzer_DB() throws IOException{
		conf = new Configuration();
	}
	
	public Netflow_Analyzer_DB(Configuration Conf) throws IOException{
		conf = Conf;
		srcFileName = conf.getStrings("job.input.srcDir")[0];
		dstFileName = conf.getStrings("job.output.dstDir")[0]+"/application";
		reducer_num = conf.getInt("job.reducer.number", 1);
		//interval = conf.getInt("netflow.analyzer.interval", 60); 
	}
	
	public void start() throws ClassNotFoundException, InterruptedException{
	        
    	try{

    	   outputDir = new Path(dstFileName + "/state1/");
    	   inputDir = new Path(srcFileName);
    		FileSystem fs = FileSystem.get(conf);
			Job job_state1 = get_JobConf("Netflow_Stat state1", inputDir, outputDir);  
			
			// delete any output that might exist from a previous run of this job
			if (fs.exists(FileOutputFormat.getOutputPath(job_state1))) {
				fs.delete(FileOutputFormat.getOutputPath(job_state1), true);
	        }

			job_state1.waitForCompletion(true);
    	}catch (IOException e) {
    		// TODO Auto-generated catch block
    		e.printStackTrace();
    	}
	}
	
	private Job get_JobConf(String jobName, Path inFilePath, Path outFilePath) throws IOException{//获取第一阶段工作配置
		  
		DBConfiguration.configureDB(conf, "com.mysql.jdbc.Driver", "jdbc:mysql://localhost:3306/netflow", "root", "");
		
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
		DBOutputFormat.setOutput(job, "Netflow_Application", "router", "timestamp", "protocol", "flows", "packets", "bytes");
		
		job.setMapperClass(Map_Stats1.class);
		job.setCombinerClass(Combine_Stats1.class);          
		job.setReducerClass(Reduce_Stats1.class);    
		
		FileInputFormat.setInputPaths(job, inFilePath);
		FileOutputFormat.setOutputPath(job, outFilePath);
        
      return job;
	}
	
	public static class Map_Stats1 extends Mapper<LongWritable, BytesWritable, Text, Text>{
		private int interval = 60;
		private byte[] value_bytes;
		
		private int src_port;
		private int dst_port;
		private long packets;
		private long bytes;
		private long timestamp;
		
		private Netflow_record record = new Netflow_record();
		
		@Override
		public void setup(Context context)throws IOException,InterruptedException{
			interval = context.getConfiguration().getInt("netflow.analyzer.interval", 60); 
		}
		
		@Override
		public void map(LongWritable key, BytesWritable value, Context context) throws IOException, InterruptedException {
			//System.out.println(flow_size++);
			//3327031
			value_bytes = value.getBytes();
			record.setNeflowRecord(value_bytes);
			
			timestamp = record.getStime();
			timestamp = timestamp - timestamp%interval;
			
			src_port = record.getSrcPort();
			
			dst_port = record.getDstPort();

			packets = record.getPKT();
			bytes = record.getBytes();
			
			//packets = packets*1000000000+bytes;
			
		   context.write(new Text(timestamp + "\t"+get_protocol_type(src_port, dst_port)), new Text(packets+"\t"+bytes));

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
			
			context.write(new NetflowDBWritable2(1, (int)timestamp, (int)protocol, (int)flows, (int)packets, bytes), null);
			  
		}
	    
	}


	public static int get_protocol_type(int port1, int port2){
		switch(port1){
		case 53:
			return 2;
		case 25:
			return 7;
		case 110:
			return 4;
		case 80:
			return 1;
		case 23:
			return 5;
		case 443:
			return 6;
		case 69:
			return 9;
		case 161:
			return 3;
		case 162:
			return 3;
		case 20:
			return 8;
		case 21:
			return 8;
		default:
			;
		}
		switch(port2){
		case 53:
			return 2;
		case 25:
			return 7;
		case 110:
			return 4;
		case 80:
			return 1;
		case 23:
			return 5;
		case 443:
			return 6;
		case 69:
			return 9;
		case 161:
			return 3;
		case 162:
			return 3;
		case 20:
			return 8;
		case 21:
			return 8;
		default:
			;
		}
		return 10;
	}
	
}
