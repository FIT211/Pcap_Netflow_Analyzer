package com.pangu.Netflow.Analyzer;

import java.io.IOException;

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

import com.pangu.Netflow.Netflow_IO.*;

public class Application_Analyzer {

	private static Configuration conf;
	private static String srcFileName;
	private static Path inputDir;
	private static int reducer_num;
	
	private static String database;
	private static String username;
	private static String password;
	
	public Application_Analyzer() throws IOException{
		conf = new Configuration();
	}
	
	public Application_Analyzer(Configuration Conf) throws IOException{
		conf = Conf;
		srcFileName = conf.getStrings("job.input.srcDir")[0];
		reducer_num = conf.getInt("job.reducer.number", 1);
		
		database = conf.get("job.mysql.database");
		username = conf.get("job.mysql.username");
		password = conf.get("job.mysql.password", "");
	}
	
	public void start() throws ClassNotFoundException, InterruptedException{
	        
    	try{

    	   inputDir = new Path(srcFileName);

			Job job_state1 = get_JobConf("Netflow_Stat state1", inputDir);  

			job_state1.submit();
			//job_state1.waitForCompletion(true);
    	}catch (IOException e) {
    		// TODO Auto-generated catch block
    		e.printStackTrace();
    	}
	}
	
	private Job get_JobConf(String jobName, Path inFilePath) throws IOException{//获取第一阶段工作配置
		  
		//DBConfiguration.configureDB(conf, "org.mariadb.jdbc.Driver", database, username, password);
		DBConfiguration.configureDB(conf, "com.mysql.jdbc.Driver", database, username, password);
		//DBConfiguration.configureDB(conf, "com.mysql.jdbc.Driver", "jdbc:mysql://166.111.143.245:3306/netflow", "root", "root");
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

			value_bytes = value.getBytes();
			record.setNeflowRecord(value_bytes);
			
			timestamp = record.getStime();
			timestamp = timestamp - timestamp%interval;
			
			src_port = record.getSrcPort();
			
			dst_port = record.getDstPort();

			packets = record.getPKT();
			bytes = record.getBytes();
			
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
				//packets += Integer.parseInt(temp[0].trim());
				//bytes += Integer.parseInt(temp[1].trim());
				packets += Long.parseLong(temp[0].trim());
				bytes += Long.parseLong(temp[1].trim());
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
		private int router;
		@Override
		public void setup(Context context){
			router = context.getConfiguration().getInt("netflow.analyzer.router", 1);
		}
		
		@Override
		public void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
			packets = 0;
			bytes = 0;
			flows = 0;
			while(value.iterator().hasNext()){
				temp = value.iterator().next().toString().split("\t");
				//flows += Integer.parseInt(temp[0].trim());
				//packets += Integer.parseInt(temp[1].trim());
				//bytes += Integer.parseInt(temp[2].trim());
				flows += Long.parseLong(temp[0].trim());
				packets += Long.parseLong(temp[1].trim());
				bytes += Long.parseLong(temp[2].trim());
			}
			
			temp = key.toString().split("\t");
			timestamp = Integer.parseInt(temp[0].trim());
			protocol = Integer.parseInt(temp[1].trim());
			
			context.write(new NetflowDBWritable2(router, (int)timestamp, (int)protocol, (int)flows, (int)packets, bytes), null);
			  
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
