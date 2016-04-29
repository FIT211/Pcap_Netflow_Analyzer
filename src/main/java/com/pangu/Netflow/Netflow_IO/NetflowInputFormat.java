package com.pangu.Netflow.Netflow_IO;

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

import com.pangu.Netflow.Netflow_IO.NetflowRecordReader;

public class NetflowInputFormat extends FileInputFormat<LongWritable, BytesWritable> implements JobConfigurable {
	
    private CompressionCodecFactory compressionCodecs = null;

    @Override
    protected boolean isSplitable(JobContext context, Path file) {
    	  this.compressionCodecs = new CompressionCodecFactory(context.getConfiguration());
        if (this.compressionCodecs.getCodec(file) == null) {
            return true;
           }
        return false;
    }
    

	@Override
	public RecordReader<LongWritable, BytesWritable> createRecordReader(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		RecordReader<LongWritable, BytesWritable> recordReader = new NetflowRecordReader();
		return recordReader;
	}

	@Override
	public void configure(JobConf arg0) {
		// TODO Auto-generated method stub
		
	}
}