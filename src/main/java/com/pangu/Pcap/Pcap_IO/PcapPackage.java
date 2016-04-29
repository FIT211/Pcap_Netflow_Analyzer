package com.pangu.Pcap.Pcap_IO;

import com.pangu.Common.BinaryUtils;
import com.pangu.Common.Bytes;
import com.pangu.Common.CommonData;

public class PcapPackage {
	private int timestamp = 0;
	private int timestamp_msc = 0;
	private int caplen = 0;
	private int wirlen = 0;
	private int ip_ver = 0;
	private int head_len = 0;
	private int len = 0;
	private int protocol = 0;
	private int src_port = 0;
	private int dst_port = 0;
	private int[] src_ip = new int[4];
	private int[] dst_ip = new int[4];
	private String str_src_ip;
	private String str_dst_ip;
	
	private byte[] timestamp_b = new byte[4];
	private byte[] timestamp_msc_b = new byte[4];
	private byte[] caplen_b = new byte[4];
	private byte[] wirlen_b = new byte[4];
	private byte[] ip_ver_b = new byte[1];
	private byte[] len_b = new byte[2];
	private byte[] protocol_b = new byte[1];
	private byte[] src_port_b = new byte[2];
	private byte[] dst_port_b = new byte[2];
	private byte[] src_ipv4_b = new byte[4];
	private byte[] dst_ipv4_b = new byte[4];
	private byte[] src_ipv6_b = new byte[16];
	private byte[] dst_ipv6_b = new byte[16];
	
	public boolean setPcapPacket(byte[] packet){
		System.arraycopy(packet, 0, timestamp_b, 0, 4);
		System.arraycopy(packet, 4, timestamp_msc_b, 0, 4);
		System.arraycopy(packet, 8, caplen_b, 0, 4);
		System.arraycopy(packet, 12, wirlen_b, 0, 4);
		
		timestamp = Bytes.toInt(BinaryUtils.flipBO(timestamp_b, 4));
		timestamp_msc = Bytes.toInt(BinaryUtils.flipBO(timestamp_msc_b, 4));
		caplen = Bytes.toInt(BinaryUtils.flipBO(caplen_b, 4));
		wirlen = Bytes.toInt(BinaryUtils.flipBO(wirlen_b, 4));

		System.arraycopy(packet, 30, ip_ver_b, 0, 1);
		
		///System.out.println("packet:"+packet.length+"\tcaplen:"+caplen);
		
		if((byte)(ip_ver_b[0]&0xf0) == 0x40){
			//TCP=6，ICMP=1，UDP=17
			ip_ver = 4;
			try{
				System.arraycopy(packet, 32, len_b, 0, 2);
				System.arraycopy(packet, 39, protocol_b, 0, 1);
				System.arraycopy(packet, 42, src_ipv4_b, 0, 4);
				System.arraycopy(packet, 46, dst_ipv4_b, 0, 4);
				
				len = Bytes.toInt(BinaryUtils.flipBO(len_b, 2));//部首和数据之和的长度
				protocol = Bytes.toInt(BinaryUtils.flipBO(protocol_b, 1));
				str_src_ip = CommonData.longTostrIp(Bytes.toLong(src_ipv4_b));
				str_dst_ip = CommonData.longTostrIp(Bytes.toLong(dst_ipv4_b));

				ip_ver_b[0] = (byte)(ip_ver_b[0]&0x0f);
				head_len = Bytes.toInt(BinaryUtils.flipBO(ip_ver_b, 1))*4;
				//System.out.println("ip_ver"+ip_ver+"\tprotocol:"+str_src_ip+"\thead_len:"+str_dst_ip);
				if(protocol == 17 || protocol == 6){
					System.arraycopy(packet, 30+head_len, src_port_b, 0, 2);
					System.arraycopy(packet, 30+head_len+2, dst_port_b, 0, 2);

					src_port = Bytes.toInt(src_port_b);
					dst_port = Bytes.toInt(dst_port_b);
				}
			}catch(ArrayIndexOutOfBoundsException e){
				//System.out.println("Package ipv4 decode error: ArrayIndexOutOfBoundsException");
				return false;
			}
			
			
		}else if((byte)(ip_ver_b[0]&0xf0) == 0x60){
			ip_ver = 6;
			try{
				//System.out.println("!!!!!!!!!!!!!!!caplen:"+caplen);
				System.arraycopy(packet, 34, len_b, 0, 2);
				System.arraycopy(packet, 36, protocol_b, 0, 1);
				System.arraycopy(packet, 38, src_ipv6_b, 0, 16);
				System.arraycopy(packet, 54, dst_ipv6_b, 0, 16);
				
				len = Bytes.toInt(BinaryUtils.flipBO(len_b, 2));//部首以外的长度
				protocol = Bytes.toInt(BinaryUtils.flipBO(protocol_b, 1));
				str_src_ip = CommonData.BytesToStrIPv6(src_ipv6_b);
				str_dst_ip = CommonData.BytesToStrIPv6(dst_ipv6_b);
				
				if(protocol == 17 && protocol == 6){
					System.arraycopy(packet, 70, src_port_b, 0, 2);
					System.arraycopy(packet, 70+2, dst_port_b, 0, 2);

					src_port = Bytes.toInt(src_port_b);
					dst_port = Bytes.toInt(dst_port_b);
				}
			}catch(ArrayIndexOutOfBoundsException e){
				//System.out.println("Package ipv6 decode error: ArrayIndexOutOfBoundsException");
				return false;
			}
			
		}else{
			ip_ver = 0;
		}
		return true;
	}
	
	public int getTimestamp(){
		return timestamp;
	}
	
	public int getTimestampMsc(){
		return timestamp_msc;
	}
	
	public int getCaplen(){
		return caplen;
	}
	
	public int getWirlen(){
		return wirlen;
	}
	
	public int getIpVer(){
		return ip_ver;
	}
	
	public int getLen(){
		return len;
	}
	
	public int getProtocol(){
		return protocol;
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
	
}

