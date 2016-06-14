package com.pangu.Netflow.Netflow_IO;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.FileSplit;

public class NetflowHiveInputFormat extends FileInputFormat<LongWritable, BytesWritable> implements JobConfigurable{
	private CompressionCodecFactory compressionCodecs = null;
	
	protected boolean isSplitable(JobContext context, Path file) {
    	  this.compressionCodecs = new CompressionCodecFactory(context.getConfiguration());
        if (this.compressionCodecs.getCodec(file) == null) {
            return true;
           }
        return false;
    }
	

	@Override
	public RecordReader<LongWritable, BytesWritable> getRecordReader(
			InputSplit inputsplit, JobConf conf,
			Reporter report) throws IOException {
		// TODO Auto-generated method stub
		report.setStatus(inputsplit.toString());
		RecordReader<LongWritable, BytesWritable> recordReader = new NetflowHiveRecordReader((Configuration)conf, (FileSplit)inputsplit);
		return recordReader;
	}

	public void configure(JobConf arg0) {
		// TODO Auto-generated method stub
		
	}

}
