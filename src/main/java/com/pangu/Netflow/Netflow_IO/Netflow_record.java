package com.pangu.Netflow.Netflow_IO;

import com.pangu.Common.*;

public class Netflow_record {
	private int type = 0;
	private int size = 0;
	private int stime = 0;
	private int etime = 0;
	private int fwd_status = 0;
	private int tcp_flag = 0;
	private int prot = 0;
	private int tos = 0;
	private int src_port = 0;
	private int dst_port = 0;
	//private int[] src_ip = new int[4];
	//private int[] dst_ip = new int[4];
	private String str_src_ip;
	private String str_dst_ip;
	private long pkt = 0;
	private long bytes = 0;
	
	private int flag_ipv6 = 0;
	private int flag_pkt64 = 0;
	
	private byte[] type_b = new byte[2];
	private byte[] size_b = new byte[2];
	private byte[] flag = new byte[2];
	private byte[] stime_b = new byte[4];
	private byte[] etime_b = new byte[4];
	private byte[] fwd_status_b = new byte[1];
	private byte[] tcp_flag_b = new byte[1];
	private byte[] prot_b = new byte[1];
	private byte[] tos_b = new byte[1];
	private byte[] src_port_b = new byte[2];
	private byte[] dst_port_b = new byte[2];
	private byte[] src_ip_v4 = new byte[4];
	private byte[] dst_ip_v4 = new byte[4];
	private byte[] src_ip_v6 = new byte[16];
	private byte[] dst_ip_v6 = new byte[16];
	private byte[] pkt_b8 = new byte[8];
	private byte[] bytes_b8 = new byte[8];
	private byte[] pkt_b4 = new byte[4];
	private byte[] bytes_b4 = new byte[4];
	
	public boolean setNeflowRecord(byte[] record){
		System.arraycopy(record, 0, type_b, 0, 2);
		System.arraycopy(record, 2, size_b, 0, 2);
		System.arraycopy(record, 4, flag, 0, 2);
		System.arraycopy(record, 12, stime_b, 0, 4);
		System.arraycopy(record, 16, etime_b, 0, 4);
		System.arraycopy(record, 20, fwd_status_b, 0, 1);
		System.arraycopy(record, 21, tcp_flag_b, 0, 1);
		System.arraycopy(record, 22, prot_b, 0, 1);
		System.arraycopy(record, 23, tos_b, 0, 1);
		System.arraycopy(record, 24, src_port_b, 0, 2);
		System.arraycopy(record, 26, dst_port_b, 0, 2);
		//System.out.println(type_b[0]+"\t"+type_b[1]);
		//System.out.println(flag[0]+"\t"+flag[1]);
		
		if((flag[0]&0x01) == 0x01){
			flag_ipv6 = 1;
			System.arraycopy(record, 28, src_ip_v6, 0, 16);
			System.arraycopy(record, 32+12, dst_ip_v6, 0, 16);
			str_dst_ip = CommonData.BytesToStrIPv6(dst_ip_v6);
			str_src_ip = CommonData.BytesToStrIPv6(src_ip_v6);
		}else{
			flag_ipv6 = 0;
			System.arraycopy(record, 28, src_ip_v4, 0, 4);
			System.arraycopy(record, 32, dst_ip_v4, 0, 4);
			str_src_ip = CommonData.longTostrIp(Bytes.toLong(BinaryUtils.flipBO(src_ip_v4, 4)));
			str_dst_ip = CommonData.longTostrIp(Bytes.toLong(BinaryUtils.flipBO(dst_ip_v4, 4)));
		}
		
		if((flag[0]&0x02) == 0x02){
			flag_pkt64 = 1;
			System.arraycopy(record, 36+flag_ipv6*24, pkt_b8, 0, 8);
			pkt = Bytes.toLong(BinaryUtils.flipBO(pkt_b8, 8));
		}else{
			flag_pkt64 = 0;
			System.arraycopy(record, 36+flag_ipv6*24, pkt_b4, 0, 4);
			pkt = Bytes.toLong(BinaryUtils.flipBO(pkt_b4, 4));
		}
		
		if((flag[0]&0x04) == 0x04){
			System.arraycopy(record, 40+flag_ipv6*24+flag_pkt64*4, bytes_b8, 0, 8);
			bytes = Bytes.toLong(BinaryUtils.flipBO(bytes_b8, 8));
		}else{
			System.arraycopy(record, 40+flag_ipv6*24+flag_pkt64*4, bytes_b4, 0, 4);
			bytes = Bytes.toLong(BinaryUtils.flipBO(bytes_b4, 4));
		}
		
		type = Bytes.toInt(BinaryUtils.flipBO(type_b, 2));
		size = Bytes.toInt(BinaryUtils.flipBO(size_b, 2));
		stime = Bytes.toInt(BinaryUtils.flipBO(stime_b, 4));
		etime = Bytes.toInt(BinaryUtils.flipBO(etime_b, 4));
		fwd_status = Bytes.toInt(BinaryUtils.flipBO(fwd_status_b, 1));
		tcp_flag = Bytes.toInt(BinaryUtils.flipBO(tcp_flag_b, 1));
		prot = Bytes.toInt(BinaryUtils.flipBO(prot_b, 1));
		tos = Bytes.toInt(BinaryUtils.flipBO(tos_b, 1));
		src_port = Bytes.toInt(BinaryUtils.flipBO(src_port_b, 2));
		dst_port = Bytes.toInt(BinaryUtils.flipBO(dst_port_b, 2));

		return true;
		
	}
	
	public int getType(){
		return type;
	}
	
	public int getSize(){
		return size;
	}
	
	public int getStime(){
		return stime;
	}
	
	public int getEtime(){
		return etime;
	}
	
	public int getFwdStatus(){
		return fwd_status;
	}
	
	public int getTcpFlag(){
		return tcp_flag;
	}
	
	public int getProto(){
		return prot;
	}
	
	public int getTos(){
		return tos;
	}
	
	public int getSrcPort(){
		return src_port;
	}
	
	public int getDstPort(){
		return dst_port;
	}
	
	public String getSrcIP(){
		return str_src_ip;
	}
	
	public String getDstIP(){
		return str_dst_ip;
	}
	
	public long getPKT(){
		return pkt;
	}
	
	public long getBytes(){
		return bytes;
	}
}
