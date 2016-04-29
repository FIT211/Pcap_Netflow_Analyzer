package com.pangu.Pcap.Runner;

import org.apache.hadoop.conf.Configuration;

import com.pangu.Pcap.Analyzer.*;

public class Pcap_analysis_runner {
	//bin/hadoop jar /home/seasun/workspace/Pcap_Netflow_Analyzer/target/Pcap_Netflow_Analyzer-0.0.1-SNAPSHOT-jar-with-dependencies.jar com.pangu.Pcap.Runner.Pcap_analysis_runner -jnetwork -r/apps/pacp_test/sample.pcap
	/*<resources>
	<resource>
     <directory>src/main/resources</directory>
     <includes>  
     <include>**//*.mmdb</include> 
     </includes>  
     <filtering>false</filtering>  
  </resource>
	</resources>*/
	private static Configuration conf;
	private static String srcFilename = null;		
	private static String dstFilename= null;			
	private static String dbDir= null;
	private static String jobType = null;
	private static int topN = 1;
	private static int interval = 60;
	
	public static void main(String[] args) throws Exception{

		conf = new Configuration();
		char argtype = 0;
		
		/* Argument Parsing */
		int i = 0;
		while(i<args.length){
			if(args[i].startsWith("-")){
				
				argtype = args[i].charAt(1);
				switch (argtype){
				case 'J': case 'j':
					jobType = args[i].substring(2);
					break;
					
				case 'R': case 'r':
					srcFilename = args[i].substring(2);
					break;
					
				case 'D': case 'd':
					dstFilename = args[i].substring(2);
					break;			
					
				case 'I': case 'i':
					interval = Integer.parseInt(args[i].substring(2).trim());
					break;	
					
				case 'N': case 'n': // topN
					topN = Integer.parseInt(args[i].substring(2).trim());
					break;
					
				case 'A': case 'a':
					dbDir = args[i].substring(2);
					break;
					
				default:
					;
				break;
				}					
			}
			else{
				
				argtype = args[i].charAt(0);
				switch (argtype){
				case 'J': case 'j':
					jobType = args[i].substring(1);
					break;
					
				case 'R': case 'r':
					srcFilename = args[i].substring(1);
					break;
					
				case 'D': case 'd':
					dstFilename = args[i].substring(1);
					break;			
					
				case 'I': case 'i':
					interval = Integer.parseInt(args[i].substring(1).trim());
					break;	
					
				case 'N': case 'n': // topN
					topN = Integer.parseInt(args[i].substring(1).trim());
					break;
					
				case 'A': case 'a':
					dbDir = args[i].substring(1);
					break;
					
				default:
					;
				break;
				}		
			}
			i++;
		}

		if(srcFilename == null){
			System.out.println("Error: SrcFileName = null. Please specify an input file!");
			return;
		}else{
			conf.setStrings("pcap.record.srcDir", srcFilename);
		}
		
		if(dstFilename == null){
			String[] tmp = srcFilename.split("/");
			dstFilename = "/results";
			for(int j = 2; j < tmp.length; j++){
				dstFilename += "/"+tmp[j];
			}
		}
		conf.setStrings("pcap.record.dstDir", dstFilename);
		
		conf.setStrings("pcap.record.dbDir", dbDir);
		conf.setInt("pcap.record.interval", interval);
		conf.setInt("pcap.record.reducer.num", topN);
		
		conf.set("fs.defaultFS", "hdfs://127.0.0.1:9000");
		
		if(jobType == null){
			System.out.println("Error: JobType = null. Please specify a job type!");
			return;
		}else{
			switch (jobType){
			case "application":
				System.out.println(JobInfo("application layer analysis"));
				Application_analyzer application_analyzer = new Application_analyzer(conf);
				application_analyzer.start();
				break;
				
			case "network":
				if(dbDir == null){
					System.out.println("Error: dbDir = null. Please specify a database dir!");
					return;
				}
				System.out.println(JobInfo("network layer analysis"));
				Network_analyzer network_analyzer = new Network_analyzer(conf);
				network_analyzer.start();
				break;
				
			case "transport":
				System.out.println(JobInfo("transport layer analysis"));
				Transport_analyzer transport_analyzer = new Transport_analyzer(conf);
				transport_analyzer.start();
				break;	
				
			default:
				;
			break;
			}		
		}
	}
	
	private static String JobInfo(String jobType){
		return "JobType:"+jobType+"\nSrcDir:"+srcFilename+"\tDstDir:"+dstFilename+"\nInterval:"+interval+"\tReducerNum:"+topN;
	}
}
