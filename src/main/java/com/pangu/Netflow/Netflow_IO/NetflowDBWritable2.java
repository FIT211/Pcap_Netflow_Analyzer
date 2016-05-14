package com.pangu.Netflow.Netflow_IO;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

public class NetflowDBWritable2 implements Writable, DBWritable{

	int router;  
	int timestamp;
	int type;
	int flows;
	int bytes;
	int packets;
	
   public NetflowDBWritable2() {  
	   
    }  
   
   public NetflowDBWritable2(int router, int timestamp, int type, int flows, int packets, int bytes) {  
	   this.router = router;
	   this.timestamp = timestamp;
	   this.type = type;
	   this.flows = flows;
	   this.bytes = bytes;
	   this.packets = packets;
    }  
   
	@Override
	public void readFields(ResultSet resultset) throws SQLException {
		// TODO Auto-generated method stub
		this.router = resultset.getInt(1);
		this.timestamp = resultset.getInt(2);
		this.type = resultset.getInt(3);
		this.flows = resultset.getInt(4);
		this.packets = resultset.getInt(5);
		this.bytes = resultset.getInt(6);
		
	}

	@Override
	public void write(PreparedStatement statement) throws SQLException {
		// TODO Auto-generated method stub
		statement.setInt(1, this.router);
		statement.setInt(2, this.timestamp);
		statement.setInt(3, this.type);
		statement.setInt(4, this.flows);
		statement.setInt(5, this.packets);
		statement.setInt(6, this.bytes);
		
	}

	@Override
	public void readFields(DataInput resultset) throws IOException {
		// TODO Auto-generated method stub
		this.router = resultset.readInt();
		this.timestamp = resultset.readInt();
		this.type = resultset.readInt();
		this.flows = resultset.readInt();
		this.packets = resultset.readInt();
		this.bytes = resultset.readInt();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeInt(router);
		out.writeInt(timestamp);
		out.writeInt(type);
		out.writeInt(flows);
		out.writeInt(packets);
		out.writeInt(bytes);
	}

}
