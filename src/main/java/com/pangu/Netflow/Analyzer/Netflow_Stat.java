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
		srcFileName = conf.getStrings("job.input.srcDir")[0];
		dstFileName = conf.getStrings("job.output.dstDir")[0]+"/Netflow_Stat";
		reducer_num = conf.getInt("job.reducer.number", 1);
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
		job.setOutputValueClass(Text.class);	       
		job.setInputFormatClass(NetflowInputFormat.class);          
		job.setOutputFormatClass(TextOutputFormat.class);     
		job.setMapperClass(Map_Stats1.class);
		job.setCombinerClass(Combine_Stats1.class);          
		job.setReducerClass(Reduce_Stats1.class);    
		
		FileInputFormat.setInputPaths(job, inFilePath);
		FileOutputFormat.setOutputPath(job, outFilePath);
        
      return job;
	}

	public static class Map_Stats1 extends Mapper<LongWritable, BytesWritable, Text, Text>{
		
		private byte[] value_bytes;
		
		private int src_port;
		private int dst_port;
		private long timestamp;
		private long packets;
		private long bytes;
		
		private Netflow_record record = new Netflow_record();
		
		@Override
		public void setup(Context context)throws IOException,InterruptedException{
		}
		
		@Override
		public void map(LongWritable key, BytesWritable value, Context context) throws IOException, InterruptedException {
			//System.out.println(flow_size++);
			//3327031
			value_bytes = value.getBytes();
			record.setNeflowRecord(value_bytes);
			
			timestamp = record.getStime();
			timestamp = timestamp - timestamp%300;
			
			src_port = record.getSrcPort();
			
			dst_port = record.getDstPort();

			packets = record.getPKT();
			bytes = record.getBytes();
			
		   context.write(new Text(timestamp + "\t"+get_protocol_type(src_port, dst_port)), new Text(packets+"\t"+bytes));
			/*	
			src_port = record.getSrcPort();
			//text.clear();Finished spill 15
			//text.set("sp" + "\t"+src_port);
			context.write(new Text("sp" + "\t"+src_port), new LongWritable(1));
			
			dst_port = record.getDstPort();
			//text.clear();
			//text.set("dp" + "\t"+dst_port);
		   context.write(new Text("dp" + "\t"+dst_port), new LongWritable(1));
		
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
		   
			protocol = record.getProto();
			//text.clear();
			//text.set("pr" + "\t"+protocol);
		   context.write(new Text("pr" + "\t"+protocol), new LongWritable(1));
		
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
	
	public static class Reduce_Stats1 extends Reducer<Text, Text, Text, Text> {	

		private long flows;
		private long packets;
		private long bytes;
		private String temp[];
		
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
			
			context.write(key, new Text(flows+"\t"+packets+"\t"+bytes));
			  
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