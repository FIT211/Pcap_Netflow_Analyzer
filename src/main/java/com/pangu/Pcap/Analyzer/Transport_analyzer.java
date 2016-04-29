package com.pangu.Pcap.Analyzer;

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

import com.pangu.Pcap.Pcap_IO.*;

public class Transport_analyzer {
		
	private static Configuration conf;
	private static String srcFileName;
	private static String dstFileName;
	private static int reducer_num;	
	private static Path outputDir;
	private static Path inputDir;
	
	public Transport_analyzer() throws IOException{
		conf = new Configuration();
	}
	
	public Transport_analyzer(Configuration Conf) throws IOException{
		conf = Conf;
		srcFileName = conf.getStrings("pcap.record.srcDir")[0];
		dstFileName = conf.getStrings("pcap.record.dstDir")[0]+"/transport_result";
		reducer_num = conf.getInt("pcap.record.reducer.num", 1);
	}
	
	public void start() throws ClassNotFoundException, InterruptedException{
        
    	try{
    		
     	   outputDir = new Path(dstFileName + "/state1/");
     	   inputDir = new Path(srcFileName);
    	    
    		FileSystem fs = FileSystem.get(conf);
			Job job_state1 = get_state1_JobConf("transport analyzer state1", inputDir, outputDir);  
			
			// delete any output that might exist from a previous run of this job
			if (fs.exists(FileOutputFormat.getOutputPath(job_state1))) {
				fs.delete(FileOutputFormat.getOutputPath(job_state1), true);
	        }

			job_state1.waitForCompletion(true);
			
			outputDir = new Path(dstFileName + "/state2/");
			inputDir = FileOutputFormat.getOutputPath(job_state1);
			
			Job job_state2 = get_state2_JobConf("transport analyzer state2", inputDir, outputDir); 
			
			// delete any output that might exist from a previous run of this job
			if (fs.exists(FileOutputFormat.getOutputPath(job_state2))) {
				fs.delete(FileOutputFormat.getOutputPath(job_state2), true);
	        }
	      job_state2.waitForCompletion(true);
	   
	        
        }catch (IOException e) {
    		// TODO Auto-generated catch block
    		e.printStackTrace();
    	}
    }
	
	private Job get_state1_JobConf(String jobName, Path inFilePath, Path outFilePath) throws IOException{//获取第一阶段工作配置
		  
		  Job job = Job.getInstance(conf);
		  job.setJarByClass(Transport_analyzer.class);
			
        job.setJobName(jobName);     
        job.setNumReduceTasks(reducer_num);       
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);	       
        job.setInputFormatClass(PcapInputFormat.class);          
        job.setOutputFormatClass(TextOutputFormat.class);     
        job.setMapperClass(Map_Stats1.class);
        //job.setCombinerClass(Reduce_Stats1.class);          
        job.setReducerClass(Reduce_Stats1.class);    
        FileInputFormat.setInputPaths(job, inFilePath);
        FileOutputFormat.setOutputPath(job, outFilePath);
        
        return job;
	}

	private Job get_state2_JobConf(String jobName, Path inFilePath, Path outFilePath) throws IOException{//获取第二阶段工作配置

		Job job = Job.getInstance(conf);
		job.setJarByClass(Transport_analyzer.class);
		
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

	   private String protocol_type;
		private long Timestamp;
		private byte[] value_bytes;
		private int interval;
		private PcapPackage packet = new PcapPackage();
	    private Text text = new Text();
		private LongWritable longwrite = new LongWritable();
		
		@Override
		public void setup(Context context)throws IOException,InterruptedException{
			interval = context.getConfiguration().getInt("pcap.record.interval", 60); 

		}
		
		@Override
		public void map(LongWritable key, BytesWritable value, Context context) throws IOException, InterruptedException {
			
			value_bytes = value.getBytes();
			//System.out.println("value:"+value.getLength()+"\tvalues:"+value_bytes.length);

			if(value_bytes.length < 42) return;//若不够长则不分析
			
			if(packet.setPcapPacket(value_bytes)){
				if(packet.getIpVer() == 4 || packet.getIpVer() == 6){
					
					if(packet.getProtocol() == 1){
						protocol_type = "ICMP";
						
					}else if(packet.getProtocol() == 6){
						
						protocol_type = "TCP";
						
					}else if(packet.getProtocol() == 17){
						
						protocol_type = "UDP";
						
					}else{
						protocol_type = "UNKNOW";
					}
				}else{
					protocol_type = "UNKNOW";
				}
				
				Timestamp = packet.getTimestamp();
				Timestamp = Timestamp - Timestamp%interval;
				
				text.clear();
				text.set(Long.toString(Timestamp) + "\t"+protocol_type);
		      longwrite.set(packet.getCaplen());
		        	
		      context.write(text, longwrite);
			}else
				return;
	    }//map
	}//Map_States1
	
	public static class Reduce_Stats1 extends Reducer<Text, LongWritable, Text, LongWritable> {	
	
		private  long sum = 0;
		private long count = 0;
		private String temp;
		
		@Override
		public void reduce(Text key, Iterable<LongWritable> value, Context context) throws IOException, InterruptedException {
			sum = 0;
			count = 0;
			while(value.iterator().hasNext()){
				sum += value.iterator().next().get();
				count ++;
			}
			temp = key.toString();
			key.clear();
			key.set("bc\t"+temp);
			context.write(key, new LongWritable(sum));
			key.clear();
			key.set("pc\t"+temp);
			context.write(key, new LongWritable(count));
			  
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

