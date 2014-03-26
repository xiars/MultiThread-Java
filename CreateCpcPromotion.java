package com.bj58.opt.cpt.refreshCensor.server;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.bj58.opt.cpt.main.entity.CpcConfig;
import com.bj58.opt.cpt.scf.CPTMainSupport;
import com.bj58.spat.scf.client.SCFInit;

public class MultiCreateCpcPromotion {
	
	private static final Log log = LogFactory.getLog(MultiCreateCpcPromotion.class);
	
	private static Map<Integer,Integer> houseMap = new HashMap<Integer,Integer>();
	
	static {
		SCFInit.init("/opt/script/jingzhun-cron/java/refresh/scf/scf.config");
		
		try {
			List<CpcConfig> list = CPTMainSupport.getCpcConfigService().getCpcConfigListByDispPid(1);

			for (CpcConfig cpcConfig : list) {
				if (houseMap.get(cpcConfig.getCateId()) == null) {
					houseMap.put(cpcConfig.getCateId(), cpcConfig.getCateId());
				}
			}
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
	}
	
	public static void main(String[] args){

		String filePath = "/opt/script/jingzhun-cron/java/refresh/createPromotion/userId_infoId";
		
		ExecutorService pool = Executors.newFixedThreadPool(14);
		
		for (int i = 1; i < 15; i++){
			MultiThread multiThread = new MultiThread(filePath, i);
			pool.execute(multiThread);
		}
		
		try {
			Thread.sleep(86400*1000);
		} catch (InterruptedException e) {
			log.error(e);
		}
	}
	
	private static class MultiThread implements Runnable{
		
		private String filePathThread;
		private int indexThread;
		
		public MultiThread(String filePath, int index){
			filePathThread = filePath + index;
			indexThread = index;
		}
		
		
		public void run(){
			log.info("init thread" + indexThread);
			
			BufferedReader br = null;
			InputStreamReader read = null;
			
			try {
				
				File f = new File(filePathThread);
				read = new InputStreamReader (new FileInputStream(f), "UTF-8");
				br = new BufferedReader(read);
				
				String inputLine;
				inputLine = br.readLine();
				
				int i = 0;
				while(inputLine != null){
					String[] inputInfo = inputLine.split("\t");
					
					if(inputInfo.length != 2){
						log.fatal("error:" + inputLine);
						break;
					}
					
					Long userId = Long.parseLong(inputInfo[0]);		
					Long entityId = Long.parseLong(inputInfo[1]);
//					createCpcPromotion(userId, entityId); 
					inputLine = br.readLine();
					log.info(">>>>>>finish:" + i + "thread index: " + indexThread);
					i++;
				}
				
				log.info("end thread" + indexThread);
			} catch(Exception e) {
				log.error(e.getMessage(), e);
			} finally {
				try {
					
					if(br != null) {
						br.close();
					}
					
					if(read != null) {
						read.close();
					}				
				} catch(IOException e) {				
					log.error(e.getMessage(), e);
				}
			}
		}
	}
}
