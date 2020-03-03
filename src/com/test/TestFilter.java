package com.test;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.junit.Test;

public class TestFilter {
	
	/**
	 * 带过滤器扫描行键
	 * @throws Exception
	 */
	@Test
	public void regexFilter() throws Exception {
		//获取hbase的环境变量参数对象
		Configuration configuration = HBaseConfiguration.create();
								
		//设置zk集群链接地址，可以只写一个
		configuration.set("hbase.zookeeper.quorum", "hadoop01:2181,hadoop02:2181,hadoop03:2181");
		
		HTable table = new HTable(configuration, "tab2");
		Scan scan = new Scan();
		//可以设定扫描的范围
		scan.setStartRow("row30".getBytes());
		scan.setStopRow("row60".getBytes());
		
		//定义过滤器，包含3的行键
		Filter filter = new RowFilter(CompareOp.EQUAL, new RegexStringComparator("^.*3.*$"));
		
		scan.setFilter(filter);
		ResultScanner resultScanner = table.getScanner(scan);
		Iterator<Result> iterator = resultScanner.iterator();
		
		while (iterator.hasNext()) {
			Result result = iterator.next();
			byte[] number  = result.getValue("cf1".getBytes(), "number".getBytes());
			
			System.out.println(new String(number));
		}
		
		table.close();
	}
}











