package com.pangu.Pcap.Pcap_IO;


import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class PcapRecordReader extends RecordReader<LongWritable, BytesWritable>{
	
	private long start;
	private long pos;
	private long end;
	private long count;

	private Configuration conf;
	private PcapLineReader in;
	private FileSplit filesplit;
	private Path file;
	private FileSystem fs;
	private FSDataInputStream fileIn;
	
	private LongWritable key = new LongWritable();
	private BytesWritable value = new BytesWritable();

	
	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		in.close();
	}

	@Override
	public LongWritable getCurrentKey() throws IOException,
			InterruptedException {
		// TODO Auto-generated method stub
		return this.key;
	}

	@Override
	public BytesWritable getCurrentValue() throws IOException,
			InterruptedException {
		// TODO Auto-generated method stub
      return this.value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		if (this.start == this.end) {
      	return 0.0f;
	   } else {
		   return Math.min(1.0f, (float)(this.pos - this.start) / (float)(this.end - this.start));
	    }
	}
		
	@Override
	public void initialize(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		this.count = 0;
		this.conf = context.getConfiguration();
		this.filesplit = (FileSplit)split;
		this.start = this.filesplit.getStart();
		this.end = this.start + split.getLength();
		this.pos = this.start;
		this.file = this.filesplit.getPath();
		
		fs = file.getFileSystem(conf);
		fileIn = fs.open(this.file);
		
		System.out.println("initialize a split");
		fileIn.seek(this.pos);
		this.in = new PcapLineReader((InputStream)fileIn, conf);
		
		int skip;
		skip = in.locatePcapHeader();
		System.out.println("skip"+skip);
		while(skip == -2 || skip == -1){
			if(skip == -1){
				this.pos = this.end;
				break;
			}
			this.pos += 1000000;
			fileIn.seek(this.pos+skip);
			this.in = new PcapLineReader((InputStream)fileIn, conf);
			skip = in.locatePcapHeader();
		}
		
		this.pos = this.pos + skip;
		System.out.println("skip to:" + this.pos+"\tskip:"+skip);
		fileIn.seek(this.pos);
		this.in = new PcapLineReader((InputStream)fileIn, conf);
        
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		// TODO Auto-generated method stub

	   int newSize = this.in.readLine(value);

	   if(newSize == -1){
		   System.out.println("number of packet:"+count);
		   return false;
	   }else if(newSize == -2){//read error, relocate is needed
		   fileIn.seek(pos);
		   this.in = new PcapLineReader((InputStream)fileIn, conf);
		   int skip;
			skip = in.locatePcapHeader();
			//System.out.println("skip"+skip+"\tcount:"+count+"\tskip"+skip);
			
			while(skip == -2 || skip == -1){
				if(skip == -1){
					this.pos = this.end;
					return false;
				}
				this.pos += 1000000;
				fileIn.seek(this.pos+skip);
				this.in = new PcapLineReader((InputStream)fileIn, conf);
				skip = in.locatePcapHeader();
			}
			this.pos = this.pos + skip;
			//System.out.println("skip to:" + this.pos+"\tskip:"+skip);
			fileIn.seek(this.pos);
			this.in = new PcapLineReader((InputStream)fileIn, conf);
			
			newSize = this.in.readLine(value);
			this.count++;
			this.pos += newSize;
			key.set(this.pos);
			return true;
	   }else {
		   this.count++;
		   this.pos += newSize;
		   key.set(this.pos);
		   return true;
	   }


	}

}
