package com.pangu.Hive;

import java.util.ArrayList;  
import java.util.Arrays;  
import java.util.List;  
import java.util.Properties;  

import org.apache.hadoop.conf.Configuration;  
import org.apache.hadoop.hive.serde.serdeConstants;  
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;  
import org.apache.hadoop.hive.serde2.SerDeStats;  
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;  
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;  
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;  
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;  
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;  
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;  
import org.apache.hadoop.io.Writable;  

import com.pangu.Netflow.Netflow_IO.Netflow_record;

public class NetflowSerDe extends AbstractSerDe{
	//ObjectInspector inspector;
	ObjectInspector inspector;
	private ArrayList<Object> row;
	int numColumns;
	List<String> colNames;
	
   private StructTypeInfo colTypeInfo;  
   private ObjectInspector colOI;  
   //private List<String> colNames;  
   //private List<Object> row = new ArrayList<Object>();  
   //private ArrayList<Object> row;
   private Netflow_record NetflowRecord = new Netflow_record();
   private String columName ;
   private Object value;
   //private byte[] record;
   
	@Override
	public Object deserialize(Writable w) throws SerDeException {
		// TODO Auto-generated method stub
		//System.out.println("size"+row.size()+"\tcolNames size:" + colNames.size());
		BytesWritable values = (BytesWritable)w;
		byte[] record;
		record = values.getBytes();
		NetflowRecord.setNeflowRecord(record);
		for(int i = 0; i < colNames.size(); i++){
			columName = colNames.get(i);
			System.out.println(i);
			switch(columName) {
				case "stime":
					value = NetflowRecord.getStime();
					break;
				case "etime":
					value = NetflowRecord.getEtime();
					break;
				case "fwd_status":
					value = NetflowRecord.getFwdStatus();
					break;
				case "tcp_flag":
					value = NetflowRecord.getTcpFlag();
					break;
				case "prot":
					value = NetflowRecord.getProto();
					break;
				case "tos":
					value = NetflowRecord.getTos();
					break;
				case "src_port":
					value = NetflowRecord.getSrcPort();
					break;
				case "dst_port":
					value = NetflowRecord.getDstPort();
					break;
				case "src_ip":
					value = 3;//NetflowRecord.getSrcIP();
					break;
				case "dst_ip":
					value = 1;//NetflowRecord.getDstIP();
					break;
				case "pkt":
					value = 2;//NetflowRecord.getPKT();
					break;
				case "bytes":
					value = 3;//NetflowRecord.getBytes();
					break;
				default:
					value = 1;//columName;
	        }
			System.out.println("sdfswwwwssdf");
			row.set(i, value);
			//row.add(i, value);
		}
		System.out.println("ffFF");
		return row;
	}

	@Override
	public ObjectInspector getObjectInspector() throws SerDeException {
		// TODO Auto-generated method stub
		//return colOI;
		return colOI;
	}

	@Override
	public SerDeStats getSerDeStats() {
		// TODO Auto-generated method stub
		System.out.println("getSerDeStats");
		return new SerDeStats();
	}

	@Override
	public void initialize(Configuration conf, Properties tbl)
			throws SerDeException {
		// TODO Auto-generated method stub
		System.out.println("ini");
		// Get a list of the table's column names.  
		String colNamesStr = tbl.getProperty(serdeConstants.LIST_COLUMNS);  
		colNames = Arrays.asList(colNamesStr.split(","));  
		System.out.println("ini colname size:" + colNames.size());
		// Get a list of TypeInfos for the columns. This list lines up with the list of column names.  
		String colTypesStr = tbl.getProperty(serdeConstants.LIST_COLUMN_TYPES);  
		List<TypeInfo> colTypes = TypeInfoUtils.getTypeInfosFromTypeString(colTypesStr);  
		
		colTypeInfo = (StructTypeInfo) TypeInfoFactory.getStructTypeInfo(colNames, colTypes);  
		colOI = TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(colTypeInfo); 
		
		//List<ObjectInspector> inspectors = new ArrayList<ObjectInspector>(colNames.size());
		row = new ArrayList<Object>(colNames.size());
      for (int c = 0; c < colNames.size(); c++) {
        //	ObjectInspector oi = TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(colTypes.get(c));
        	//inspectors.add(oi);
         row.add(null);
        }
      //colOI = ObjectInspectorFactory.getStandardStructObjectInspector(colNames, inspectors);
		System.out.println("ini offfsdfdfk");
		
	}

	@Override
	public Class<? extends Writable> getSerializedClass() {
		// TODO Auto-generated method stub
		System.out.println("getSerializedClass");
		///??????????这个方法的作用是什么？
		return Text.class;
	}

	@Override
	public Writable serialize(Object arg0, ObjectInspector arg1)
			throws SerDeException {
		System.out.println("serialize");
		// TODO Auto-generated method stub
		return null;
	}

}
