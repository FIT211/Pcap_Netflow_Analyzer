package com.pangu.Netflow.Netflow_IO;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

public class NetflowDBWritable implements Writable, DBWritable{

	int router;  
	int drection;
	int timestamp;
	int type;
	int flows;
	long bytes;
	int packets;
	
   public NetflowDBWritable() {  
	   
    }  
   
   public NetflowDBWritable(int router, int drection, int timestamp, int type, int flows, int packets, long bytes) {  
	   this.router = router;
	   this.drection = drection;
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
		this.drection = resultset.getInt(2);
		this.timestamp = resultset.getInt(3);
		this.type = resultset.getInt(4);
		this.flows = resultset.getInt(5);
		this.packets = resultset.getInt(6);
		this.bytes = resultset.getLong(7);
		
	}

	@Override
	public void write(PreparedStatement statement) throws SQLException {
		// TODO Auto-generated method stub
		statement.setInt(1, this.router);
		statement.setInt(2, this.drection);
		statement.setInt(3, this.timestamp);
		statement.setInt(4, this.type);
		statement.setInt(5, this.flows);
		statement.setInt(6, this.packets);
		statement.setLong(7, this.bytes);
		
	}

	@Override
	public void readFields(DataInput resultset) throws IOException {
		// TODO Auto-generated method stub
		this.router = resultset.readInt();
		this.drection = resultset.readInt();
		this.timestamp = resultset.readInt();
		this.type = resultset.readInt();
		this.flows = resultset.readInt();
		this.packets = resultset.readInt();
		this.bytes = resultset.readLong();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeInt(router);
		out.writeInt(drection);
		out.writeInt(timestamp);
		out.writeInt(type);
		out.writeInt(flows);
		out.writeInt(packets);
		out.writeLong(bytes);
	}

}
