package com.pangu.Pcap.Pcap_IO;

import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;

import com.pangu.Common.*;

public class PcapLineReader {

    private InputStream in;
    private byte[] buffer;
    byte[] pcap_header = new byte[16];
    private int bufferLength = 0;
    int consumed = 0;
    int count = 0;
    private byte[] packet;

    public PcapLineReader(InputStream in, long min_captime, long max_captime) {
        this.in = in;
        this.buffer = new byte[65535+30];
    }

    public PcapLineReader(InputStream in, Configuration conf) throws IOException {
        this(in, conf.getLong("pcap.file.captime.min", 1000000000), conf.getLong("pcap.file.captime.max", 2000000000));
    }

    public void close() throws IOException {
        this.in.close();
    }


    int locatePcapHeader() throws IOException{
    	int bytes_read = 0;
    	int buffer_len = 1000000;
    	int size = 0;
    	int pos = 0;
    	
    	byte[] captured = new byte[buffer_len];
    	byte[] tmp = new byte[10000];
    	byte[] block_header1 = new byte[16];
    	byte[] block_header2 = new byte[16];
    	
    	long caplen;
    	
    	byte[] caplen_b = new byte[4];
    	//System.out.println("locating");
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
    	
    	while(pos+16 < buffer_len){
    		
    		System.arraycopy(captured, pos, block_header1, 0, 16);
    		
    		if(checkPcapHeader(block_header1)){
    			
    			System.arraycopy(block_header1, 8, caplen_b, 0, 4);
    			caplen = Bytes.toLong(BinaryUtils.flipBO(caplen_b, 4));
    			
    			if(pos+caplen+32 < buffer_len){

    	    		//System.out.println("\tcaplen:"+caplen+"\tskip"+pos);
        			System.arraycopy(captured, (int) (pos+caplen+16), block_header2, 0, 16);
        			if(checkPcapHeader(block_header2))
        				return pos;
    			}
    			
    		}
    		pos++;
    	}
    	return -2;
    }
    
    boolean checkPcapHeader(byte[] pcap_header){
    	long timestamp = 0;
    	long caplen = 0;
    	long len = 0;
    	
    	byte[] timestamp_b = new byte[4];
    	byte[] caplen_b = new byte[4];
    	byte[] len_b = new byte[4];
    	
    	System.arraycopy(pcap_header, 0, timestamp_b, 0, 4);
    	System.arraycopy(pcap_header, 8, caplen_b, 0, 4);
    	System.arraycopy(pcap_header, 12, len_b, 0, 4);

    	timestamp = Bytes.toLong(BinaryUtils.flipBO(timestamp_b, 4));
    	caplen = Bytes.toLong(BinaryUtils.flipBO(caplen_b, 4));
    	len = Bytes.toInt(BinaryUtils.flipBO(len_b, 4));
    	//if(1400000000 <= timestamp && timestamp <= 1544081701 && caplen <= len)
    		//System.out.println("time:"+timestamp+"\tcaplen:"+caplen+"\tlen:"+len);
    	
		if(1000000000 <= timestamp && timestamp <= 1744081701 && 0 < caplen && caplen < 65536 && 0 < len && len < 65536 && caplen <= len){
			
			return true;
		}
			
    	
    	return false;
    }
    
    public int readPacket() throws IOException{
    	count++;
		byte[] packet_size_b = new byte[4];
		int packet_size = 0;
		
		int bytes_read = 0;
    	int size = 0;
    	
    	byte[] tmp = new byte[100];
    	
    	in.read(pcap_header);
    	
    	System.arraycopy(pcap_header, 0, this.packet, bytes_read, 16);
    	System.arraycopy(pcap_header, 8, packet_size_b, 0, 4);

    	packet_size = Bytes.toInt(BinaryUtils.flipBO(packet_size_b, 4));
    	
    	if(0 > packet_size || packet_size > 65535){
    		//System.out.println("packet_size > 65535");
    		return -2;
    	}
    		
    	//read blok_body, size of block_size
    	
    	while(bytes_read < packet_size){
    		
    		if(packet_size -bytes_read > 100)
    			size = in.read(tmp, 0, 100);
    		else
    			size = in.read(tmp, 0, packet_size-bytes_read);
    		
    		if(size == -1){
        		return -1;
    		}
    		
    		System.arraycopy(tmp, 0, this.packet, bytes_read+16, size);
    		bytes_read+=size;

    	}
    	//System.out.println("count:"+count+"\tpacket:"+packet_size+"\tbytes_read"+bytes_read);
		return packet_size+16;
    }
    
    public int readLine(BytesWritable bytes) throws IOException {
    	bytes.set(new BytesWritable());
		this.packet = new byte[65535];
		
		int record_size = this.readPacket();
		
		if(record_size == -1){
			return -1;
		}else if(record_size == -2){
			return -2;
		}
			
		
		bytes.set(this.packet, 0, record_size);
		return record_size;
		
    }
    
    
}

