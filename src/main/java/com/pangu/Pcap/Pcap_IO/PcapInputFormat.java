/*
 * Decompiled with CFR 0_102.
 * 
 * Could not load the following classes:
 *  org.apache.hadoop.conf.Configuration
 *  org.apache.hadoop.fs.FileSystem
 *  org.apache.hadoop.fs.Path
 *  org.apache.hadoop.io.BytesWritable
 *  org.apache.hadoop.io.LongWritable
 *  org.apache.hadoop.io.compress.CompressionCodec
 *  org.apache.hadoop.io.compress.CompressionCodecFactory
 *  org.apache.hadoop.mapred.FileInputFormat
 *  org.apache.hadoop.mapred.FileSplit
 *  org.apache.hadoop.mapred.InputSplit
 *  org.apache.hadoop.mapred.JobConf
 *  org.apache.hadoop.mapred.JobConfigurable
 *  org.apache.hadoop.mapred.RecordReader
 *  org.apache.hadoop.mapred.Reporter
 */
package com.pangu.Pcap.Pcap_IO;

import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.compress.CompressionCodecFactory;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import com.pangu.Pcap.Pcap_IO.PcapRecordReader;

public class PcapInputFormat extends FileInputFormat<LongWritable, BytesWritable> implements JobConfigurable {
	
    private CompressionCodecFactory compressionCodecs = null;
    
    /*@Override
    public void configure(JobConf conf) {	
        this.compressionCodecs = new CompressionCodecFactory((Configuration)conf);
    }*/
    @Override
    protected boolean isSplitable(JobContext context, Path file) {
    	  this.compressionCodecs = new CompressionCodecFactory(context.getConfiguration());
        if (this.compressionCodecs.getCodec(file) == null) {
        		System.out.println("yes");
            return true;
           }
		  System.out.println("no");
        return false;
    }

	@Override
	public RecordReader<LongWritable, BytesWritable> createRecordReader(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		RecordReader<LongWritable, BytesWritable> recordReader = new PcapRecordReader();
		return recordReader;
	}

	@Override
	public void configure(JobConf arg0) {
		// TODO Auto-generated method stub
		
	}
}

