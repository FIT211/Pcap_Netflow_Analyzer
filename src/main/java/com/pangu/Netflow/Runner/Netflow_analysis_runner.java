package com.pangu.Netflow.Runner;


import java.io.File;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import com.pangu.Netflow.Analyzer.*;

public class Netflow_analysis_runner {
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

		char argtype = 0;
		conf = new Configuration();
		//conf.addResource(Netflow_analysis_runner.class.getClass().getResource("/configuration.xml"));

		File p = new File("/opt/hadoop-2.6.3/configuration.xml");
		conf.addResource(p.toURL());
/*		
		interval = conf.getInt("netflow.analyzer.interval", 60);
		topN = conf.getInt("job.reducer.number", 1);
		srcFilename = conf.get("job.input.srcDir");
		dstFilename = conf.get("job.output.dstDir");
		dbDir = conf.get("job.database.dbDir");

		String database = conf.get("job.mysql.database");
		String username = conf.get("job.mysql.username");
		String password = conf.get("job.mysql.password", "");
		System.out.println(database+"\t"+username+"\t1"+password+"1");
*/		/* Argument Parsing */
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
					conf.setStrings("job.input.srcDir", srcFilename);
					break;
					
				case 'D': case 'd':
					dstFilename = args[i].substring(2);
					conf.setStrings("job.output.dstDir", dstFilename);
					break;			
					
				case 'I': case 'i':
					interval = Integer.parseInt(args[i].substring(2).trim());
					conf.setInt("netflow.analyzer.interval", interval);
					break;	
					
				case 'N': case 'n': // topN
					topN = Integer.parseInt(args[i].substring(2).trim());
					conf.setInt("job.reducer.number", topN);
					break;
					
				case 'A': case 'a':
					dbDir = args[i].substring(2);
					conf.setStrings("job.database.dbDir", dbDir);
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
					conf.setStrings("job.input.srcDir", srcFilename);
					break;
					
				case 'D': case 'd':
					dstFilename = args[i].substring(1);
					conf.setStrings("job.output.dstDir", dstFilename);
					break;			
					
				case 'I': case 'i':
					interval = Integer.parseInt(args[i].substring(1).trim());
					conf.setInt("netflow.analyzer.interval", interval);
					break;	
					
				case 'N': case 'n': // topN
					topN = Integer.parseInt(args[i].substring(1).trim());
					conf.setInt("job.reducer.number", topN);
					break;
					
				case 'A': case 'a':
					dbDir = args[i].substring(1);
					conf.setStrings("job.database.dbDir", dbDir);
					break;
					
				default:
					;
				break;
				}		
			}
			i++;
		}

		if(dstFilename == null){
			srcFilename = conf.get("job.input.srcDir");
			String[] tmp = srcFilename.split("/");
			dstFilename = "/results";
			for(int j = 2; j < tmp.length; j++){
				dstFilename += "/"+tmp[j];
			}
			conf.setStrings("job.output.dstDir", dstFilename);
		}
		
		//conf.set("fs.defaultFS", "hdfs://127.0.0.1:9000");
		
		if(jobType == null){
			System.out.println("Error: JobType = null. Please specify a job type!");
			return;
		}else{
			switch (jobType){
			case "netflow_stat":
				System.out.println(JobInfo("Netflow_Stat"));
				Netflow_Stat netflow_stat = new Netflow_Stat(conf);
				netflow_stat.start();
				break;
			case "application":
				System.out.println(JobInfo("Application_Analyzer"));
				Application_Analyzer application_analyzer = new Application_Analyzer(conf);
				application_analyzer.start();
				break;
			case "network":
				System.out.println(JobInfo("Network_Analyzer"));
				Network_Analyzer network_analyzer = new Network_Analyzer(conf);
				network_analyzer.start();
				break;
			case "transport":
				System.out.println(JobInfo("Transport_Analyzer"));
				Transport_Analyzer transport_analyzer = new Transport_Analyzer(conf);
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
