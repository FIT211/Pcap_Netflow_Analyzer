package com.pangu.Netflow.Netflow_IO;


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

import com.pangu.Netflow.Netflow_IO.NetflowLineReader;

public class NetflowRecordReader extends RecordReader<LongWritable, BytesWritable>{
	
	private long start;
	private long pos;
	private long end;
	private boolean headerfound;

	private Configuration conf;
	private NetflowLineReader in;
	private FileSplit filesplit;
	private Path file;
	private FileSystem fs;
	private FSDataInputStream fileIn;
	
	private LongWritable key = new LongWritable();
	private BytesWritable value =  new BytesWritable();
 
	private int count;
	
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
		
		headerfound = true;
		if(this.end - this.start < 3000000)
			headerfound = false;
		System.out.println("Start:"+start+"\tEnd:"+end);
		
		fs = file.getFileSystem(conf);
		fileIn = fs.open(this.file);
		
		fileIn.seek(this.pos);
		this.in = new NetflowLineReader((InputStream)fileIn, conf);
		
		int skip;
		skip = in.locateBlockHeader();

		while(skip == -2 || skip == -1){
			if(skip == -1){
				this.pos = this.end;
				break;
			}
			this.pos += 1000000;
			fileIn.seek(this.pos+skip);
			this.in = new NetflowLineReader((InputStream)fileIn, conf);
			skip = in.locateBlockHeader();
		}
		
		this.pos = this.pos + skip;
		System.out.println("skip to:" + this.pos+"\tskip:"+skip);
		fileIn.seek(this.pos);
		this.in = new NetflowLineReader((InputStream)fileIn, conf);

		in.setBlock();
		
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		if(headerfound == false){
			System.out.println("header found false:"+count);
			return false;
		}
	   int newSize = this.in.readLine(value);

	   if(pos > end){
		   System.out.println("num of record:"+count+"\tpos:"+pos);
		   return false;
	   }
	   
	   if(newSize == -1){
		   System.out.println("num of record:"+count+"\tpos:"+pos);
		   return false;
	   }
	   else {
		   this.pos += newSize;
		   this.count ++;
		   key.set(this.count);
		   //System.out.println("newSize"+newSize+"\tvalue:"+value.getLength());
		   return true;
	   }

	}

}