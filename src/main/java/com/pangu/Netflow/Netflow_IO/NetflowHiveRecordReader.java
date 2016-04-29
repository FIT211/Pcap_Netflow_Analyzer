package com.pangu.Netflow.Netflow_IO;

import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.RecordReader;

import com.pangu.Netflow.Netflow_IO.NetflowLineReader;

public class NetflowHiveRecordReader implements RecordReader<LongWritable, BytesWritable>{
	
	private CompressionCodecFactory compressionCodecs = null;
	
	private long start;
	private long pos;
	private long end;
	private int count;

	private Configuration conf;
	private NetflowLineReader in;
	private FileSplit filesplit;
	private Path file;
	private FileSystem fs;
	private FSDataInputStream fileIn;

	private LongWritable key;
	private BytesWritable value;
	
	public NetflowHiveRecordReader(Configuration job, FileSplit split) throws IOException {
		this.count = 0;
		this.conf = job;
		this.filesplit = (FileSplit)split;
		this.start = this.filesplit.getStart();
		this.end = this.start + split.getLength();
		this.pos = this.start;
		this.file = this.filesplit.getPath();
		
		this.compressionCodecs = new CompressionCodecFactory(conf);
		CompressionCodec codec = this.compressionCodecs.getCodec(file);
		
		fs = file.getFileSystem(conf);
		fileIn = fs.open(this.file);
		
		if (codec != null) {
		    this.in = new NetflowLineReader((InputStream)codec.createInputStream((InputStream)fileIn), conf);
		} else {
			 fileIn.seek(this.pos);
		    this.in = new NetflowLineReader((InputStream)fileIn, conf);
		}

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
		System.out.println("fock:skip to:" + this.pos+"\tskip:"+skip);
		//System.out.println("hi");
		fileIn.seek(this.pos);
		this.in = new NetflowLineReader((InputStream)fileIn, conf);
		//System.out.println("hello");
		in.setBlock();
	}
	
	//@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		in.close();
	}
	//@Override
	public LongWritable createKey() {
		// TODO Auto-generated method stub
		return new LongWritable();
	}
	//@Override
	public BytesWritable createValue() {
		// TODO Auto-generated method stub
		return new BytesWritable();
	}
    
	//@Override
	public long getPos() throws IOException {
		// TODO Auto-generated method stub
		return 0;
	}
	//@Override
	public float getProgress() throws IOException {
		// TODO Auto-generated method stub
		if (this.start == this.end) {
      	return 0.0f;
	   } else {
		   return Math.min(1.0f, (float)(this.pos - this.start) / (float)(this.end - this.start));
	    }
	}
	//@Override
	public boolean next(LongWritable key, BytesWritable value)
			throws IOException {
		// TODO Auto-generated method stub
		/*if (key == null) {
			key = new LongWritable();
	    }
	   if (value == null) {
		   value = new BytesWritable();
	    }*/
	   
	   int newSize = this.in.readLine(value);

	   if(newSize == -1)
		   return false;
	   else {
		   this.pos += newSize;
		   this.count ++;
		   key.set(this.count);
		   //System.out.println("return next");
		   return true;
	   }
	}
}
