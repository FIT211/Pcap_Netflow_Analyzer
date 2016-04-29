package com.pangu.Netflow.Netflow_IO;


import java.io.IOException;
import java.io.InputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;

import com.pangu.Common.*;

import org.anarres.lzo.LzoDecompressor;
import org.anarres.lzo.LzoLibrary;
import org.anarres.lzo.lzo_uintp;
import org.anarres.lzo.LzoAlgorithm;

public class NetflowLineReader {

    private InputStream in;
    
    private int record_num = 0;
    private int block_size = 0;
    private int record_read = 0;
    private int pos = 0;
    
    private byte[] block_header = new byte[12];
    private byte[] block_body_compressed;
    private byte[] block_body = new byte[4*500000];
    private byte[] record = new byte[1000];
    
    public NetflowLineReader(InputStream in, long min_captime, long max_captime) {
        this.in = in;
    }

    public NetflowLineReader(InputStream in, Configuration conf) throws IOException {
        //this(in, conf.getLong("pcap.file.captime.min", 1000000000), conf.getLong("pcap.file.captime.max", 2000000000));
    	this.in = in;
    }

    public void close() throws IOException {
        this.in.close();
    }

    int locateBlockHeader() throws IOException{
    	int bytes_read = 0;
    	int buffer_len = 1000000;
    	int size = 0;
    	int pos = 0;
    	
    	byte[] captured = new byte[buffer_len];
    	byte[] tmp = new byte[10000];
    	byte[] block_header = new byte[12];
    	
    	//get buffer from in
    	while(bytes_read < buffer_len){
    		size = in.read(tmp);
    		if(size == -1)
        		return -1;
        	if(size < buffer_len-bytes_read){
        		System.arraycopy(tmp, 0, captured, bytes_read, size);
        		bytes_read += size;
        	}
        	else{
        		System.arraycopy(tmp, 0, captured, bytes_read, buffer_len-bytes_read);
        		bytes_read += size;
        	}
    	}
    	
    	while(pos < buffer_len){
    		
    		System.arraycopy(captured, pos, block_header, 0, 12);
    		
    		if(checkBlockHeader(block_header)){
    			
    			long block_size = 0;
    			byte[] block_size_b = new byte[4];
    			System.arraycopy(block_header, 4, block_size_b, 0, 4);
    			block_size = Bytes.toLong(BinaryUtils.flipBO(block_size_b, 4));
    			
    			if(pos+block_size < buffer_len){
    				
        			byte[] block_body = new byte[(int) block_size];
        			System.arraycopy(captured, pos+12, block_body, 0, (int) block_size);
        			if(checkBlockBody(block_body))
        				return pos;
    			}
    			
    		}
    		pos++;
    	}
    	return 0;
    }

    
    boolean checkBlockHeader(byte[] BlockHeader){
    	long record_num = 0;
    	long block_size = 0;
    	int id = 0;
    	int flag = 0;
    	
    	byte[] record_num_b = new byte[4];
    	byte[] block_size_b = new byte[4];
    	byte[] id_b = new byte[2];
    	byte[] flag_b = new byte[2];
    	
    	System.arraycopy(BlockHeader, 0, record_num_b, 0, 4);
    	System.arraycopy(BlockHeader, 4, block_size_b, 0, 4);
    	System.arraycopy(BlockHeader, 8, id_b, 0, 2);
    	System.arraycopy(BlockHeader, 10, flag_b, 0, 2);

    	record_num = Bytes.toLong(BinaryUtils.flipBO(record_num_b, 4));
    	block_size = Bytes.toLong(BinaryUtils.flipBO(block_size_b, 4));
		id = Bytes.toInt(BinaryUtils.flipBO(id_b, 2));
		flag = Bytes.toInt(BinaryUtils.flipBO(flag_b, 2));
		//if(id == 2 && flag == 0)
			//System.out.println("record_num:"+record_num+"\tblock_size:"+block_size+"\tid:"+id+"\tflag:"+flag);
		if(id == 2 && flag == 0 && 0 < record_num && record_num < 100000 && 0 < block_size && block_size < 1000000)
			return true;
    	
    	return false;
    }
    
    boolean checkBlockBody(byte[] BlockBody){
    	int type = 0;
    	int size = 0;
    	int flag = 0;
    	int stime = 0;
    	int etime = 0;
    	
    	byte[] type_b = new byte[2];
    	byte[] size_b = new byte[2];
    	byte[] flag_b = new byte[1];
    	byte[] stime_b = new byte[4];
    	byte[] etime_b = new byte[4];
    	
		byte[] dBlockBody = new byte[4*BlockBody.length];
		LzoAlgorithm algorithm = LzoAlgorithm.LZO1X;  
		LzoDecompressor decompressor = LzoLibrary.getInstance().newDecompressor(algorithm, null);  
		lzo_uintp len = new lzo_uintp();
		len.value = dBlockBody.length;
		decompressor.decompress(BlockBody, 0, BlockBody.length, dBlockBody, 0, len);
		
		System.arraycopy(dBlockBody, 0, type_b, 0, 2);
    	System.arraycopy(dBlockBody, 2, size_b, 0, 2);
    	System.arraycopy(dBlockBody, 4, flag_b, 0, 1);
    	System.arraycopy(dBlockBody, 12, stime_b, 0, 4);
    	System.arraycopy(dBlockBody, 16, etime_b, 0, 4);

    	type = Bytes.toInt(BinaryUtils.flipBO(type_b, 2));
    	size = Bytes.toInt(BinaryUtils.flipBO(size_b, 2));
    	flag = Bytes.toInt(flag_b);
    	stime = Bytes.toInt(BinaryUtils.flipBO(stime_b, 4));
    	etime = Bytes.toInt(BinaryUtils.flipBO(etime_b, 4));
		
    	if(type == 1 && flag == 6 && 0 < size && size < 1000 && stime <= etime && 1000000000 < stime && stime < 2000000000 && 1000000000 < etime && etime < 2000000000)
    		return true;
    	
    	return false;
    }
    
    int setBlock() throws IOException{
    	int bytes_read = 0;
    	int size = 0;
    	this.pos = 0;
    	this.record_read = 0;
    	
    	byte[] tmp = new byte[1000];
    	byte[] record_num_b = new byte[4];
    	byte[] block_size_b = new byte[4];
    	
    	in.read(block_header);
    	
    	System.arraycopy(block_header, 0, record_num_b, 0, 4);
    	System.arraycopy(block_header, 4, block_size_b, 0, 4);

    	record_num = Bytes.toInt(BinaryUtils.flipBO(record_num_b, 4));
    	block_size = Bytes.toInt(BinaryUtils.flipBO(block_size_b, 4));
    	//System.out.println("setblock\trecord_num"+record_num+"\tblock_size"+block_size);
    	block_body_compressed = new byte[block_size];
    	
    	//read blok_body, size of block_size
    	while(bytes_read < block_size){
    		
    		if(block_size -bytes_read > 100)
    			size = in.read(tmp, 0, 100);
    		else
    			size = in.read(tmp, 0, block_size-bytes_read);
    		//System.out.println("bytes_read:"+bytes_read+"\tblock_size"+block_size);
    		
    		if(size == -1){
        		return -1;
    		}
    		
    		System.arraycopy(tmp, 0, block_body_compressed, bytes_read, size);
    		bytes_read+=size;

    	}
    	
    	//decompress block_body
		LzoAlgorithm algorithm = LzoAlgorithm.LZO1X;  
		LzoDecompressor decompressor = LzoLibrary.getInstance().newDecompressor(algorithm, null);  
		lzo_uintp len = new lzo_uintp();
		len.value = 4*block_size;
		decompressor.decompress(block_body_compressed, 0, block_size, block_body, 0, len);
    	
		return 0;
    }

	int readRecord() throws IOException{
		byte[] record_size_b = new byte[2];
		int record_size = 0;
		
		if(record_num == record_read){
			if(setBlock() == -1)
				return -1;
		}
		
		System.arraycopy(block_body, pos+2, record_size_b, 0, 2);
		record_size = Bytes.toInt(BinaryUtils.flipBO(record_size_b, 2));
		
		System.arraycopy(block_body, pos, record, 0, record_size);
		pos+=record_size;
		record_read++;
		return record_size;
	}
	
	
	public int readLine(BytesWritable bytes) throws IOException {
		bytes.set(new BytesWritable());
		this.record = new byte[1000];
		
		int record_size = this.readRecord();
		
		if(record_size == -1){
			return -1;
		}
		
		bytes.set(this.record, 0, record_size);
		return record_size;
		
	}
    
}
