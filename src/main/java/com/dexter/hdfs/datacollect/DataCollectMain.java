package com.dexter.hdfs.datacollect;

import com.constant.SysConstant;

import java.util.Timer;

/**
 * 通过timer简单实习定时功能
 */
public class DataCollectMain {
	
	public static void main(String[] args) {


		Timer timer = new Timer();

		timer.schedule(new CollectTask(), 0, SysConstant.oneHour);
		
		timer.schedule(new BackupCleanTask(), 0, SysConstant.oneHour);
		
	}
	

}
