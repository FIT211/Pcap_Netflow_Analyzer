package com.pangu.Netflow.Analyzer;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;

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

import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.exception.GeoIp2Exception;
import com.maxmind.geoip2.model.CountryResponse;
import com.maxmind.geoip2.record.Country;
import com.pangu.Netflow.Netflow_IO.*;

public class Network_Analyzer {

	private static Configuration conf;
	private static String srcFileName;
	private static Path inputDir;
	private static int reducer_num;
	
	private static String database;
	private static String username;
	private static String password;
	
	public Network_Analyzer() throws IOException{
		conf = new Configuration();
	}
	
	public Network_Analyzer(Configuration Conf) throws IOException{
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
		DBOutputFormat.setOutput(job, "Netflow_Network", "router", "direction", "timestamp", "country", "flows", "packets", "bytes");
		
		job.setMapperClass(Map_Stats1.class);
		job.setCombinerClass(Combine_Stats1.class);          
		job.setReducerClass(Reduce_Stats1.class);    
		
		FileInputFormat.setInputPaths(job, inFilePath);
        
      return job;
	}
	
	public static class Map_Stats1 extends Mapper<LongWritable, BytesWritable, Text, Text>{
		private int interval = 60;
		private byte[] value_bytes;
		
		private String src_ip;
		private String dst_ip;
		private int src_country;
		private int dst_country;
		private long packets;
		private long bytes;
		private long timestamp;
		
		private String dbDir;
		private InetAddress ipAddress;
		private File database;
		private DatabaseReader reader;
		private CountryResponse response;
		private Country country;
		private boolean exist;
		
		private Netflow_record record = new Netflow_record();
		
		@Override
		public void setup(Context context)throws IOException,InterruptedException{
			interval = context.getConfiguration().getInt("netflow.analyzer.interval", 60); 
			
			dbDir = context.getConfiguration().getStrings("job.database.dbDir")[0];
			database = new File(dbDir);

			 try {
				reader = new DatabaseReader.Builder(database).build();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				System.out.println("Error Database could not be set. Please specify a valid database dir");
				e.printStackTrace();
			}
			 
			timestamp = 0;
		}
		
		@Override
		public void map(LongWritable key, BytesWritable value, Context context) throws IOException, InterruptedException {

			value_bytes = value.getBytes();
			record.setNeflowRecord(value_bytes);
			
			timestamp = record.getStime();
			timestamp = timestamp - timestamp%interval;

			packets = record.getPKT();
			bytes = record.getBytes();
			
			src_ip = record.getSrcIP();
			dst_ip = record.getDstIP();

			ipAddress = InetAddress.getByName(src_ip);
			try {
				response = reader.country(ipAddress);
				exist = true;
			} catch (GeoIp2Exception e) {
				exist = false;
			}
			if(exist == true){
    			country = response.getCountry();
    			src_country = get_country(country.getName());
    			
			}else{
				src_country = 10;
			}

			ipAddress = InetAddress.getByName(dst_ip);
			try {
				response = reader.country(ipAddress);
				exist = true;
			} catch (GeoIp2Exception e) {
				exist = false;
			}
			if(exist == true){
    			country = response.getCountry();
    			dst_country = get_country(country.getName());
        
			}else{
				dst_country = 10;
			}
			
		   context.write(new Text("0\t" + timestamp + "\t" + src_country), new Text(packets+"\t"+bytes));
		   context.write(new Text("1\t" + timestamp + "\t" + dst_country), new Text(packets+"\t"+bytes));
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
			//System.out.println(count++);
		}
	    
	}
	
	public static class Reduce_Stats1 extends Reducer<Text, Text, NetflowDBWritable, NetflowDBWritable> {	
	
		private long flows;
		private long packets;
		private long bytes;
		private String temp[];
		private long timestamp;
		private int country;
		private int direction;
		
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
			direction = Integer.parseInt(temp[0].trim());
			timestamp = Integer.parseInt(temp[1].trim());
			country = Integer.parseInt(temp[2].trim());
			
			context.write(new NetflowDBWritable(1, direction, (int)timestamp, (int)country, (int)flows, (int)packets, (int)bytes), null);
			  
		}
	    
	}
	
/*
	1.	CHINA
	2.	RUSSIA
	3.	CANADA
	4.	GERMANY
	5.	JAPAN
	6.	US
	7.	UK
	8.	INDIA
	9.	HK
	10.	OTHERS
*/
	
	public static int get_country(String country){
		if(country != null){
			switch(country){
    		case "China":
    			return 1;
    		case "Russia":
    			return 2;
    		case "Canada":
    			return 3;
    		case "Germany":
    			return 4;
    		case "Japan":
    			return 5;
    		case "United States":
    			return 6;
    		case "United Kingdom":
    			return 7;
    		case "India":
    			return 8;
    		case "Hong Kong":
    			return 9;
    		default:
    			return 10;
    		}
		}else
			return 10;
	}
	
}
