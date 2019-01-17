package com.dexter.hdfs.datacollect;

import lombok.extern.slf4j.Slf4j;

import java.util.Properties;

/**
 * 读取配置文件使用单例设计模式
 *
 */
@Slf4j
public class PropertyHolderSingle {

	private static Properties prop = new Properties();

	static {
		try {
			prop.load(PropertyHolderSingle.class.getClassLoader().getResourceAsStream("collect.properties"));
		} catch (Exception e) {
			log.error("读取配置文件失败",e);
		}
	}

	public static Properties getProps() throws Exception {
		return prop;
	}

}
