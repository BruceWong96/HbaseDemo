package com.test;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
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
		
//		HTable table = new HTable(configuration, "tab2");
		HTable table = new HTable(configuration, "tab3");
		Scan scan = new Scan();
		//可以设定扫描的范围
//		scan.setStartRow("row30".getBytes());
//		scan.setStopRow("row60".getBytes());
		
		//定义过滤器，包含3的行键
//		Filter filter = new RowFilter(CompareOp.EQUAL, new RegexStringComparator("^.*3.*$"));
		
		//行键比较过滤器 1.等于  2.小于  3.大于  4.小于等于  5.大于等于
//		Filter filter = new RowFilter(CompareOp.LESS_OR_EQUAL, new BinaryComparator("row90".getBytes()));
		
		//行键前缀过滤器
//		Filter filter = new PrefixFilter("row3".getBytes());
		
		//--列值过滤器，匹配指定符合列值的所有行数据
		Filter filter = new SingleColumnValueFilter("cf1".getBytes(), "name".getBytes(), CompareOp.EQUAL, "rose".getBytes());
		
		scan.setFilter(filter);
		ResultScanner resultScanner = table.getScanner(scan);
		Iterator<Result> iterator = resultScanner.iterator();
		
		while (iterator.hasNext()) {
			Result result = iterator.next();
			byte[] name  = result.getValue("cf1".getBytes(), "name".getBytes());
			byte[] age  = result.getValue("cf1".getBytes(), "age".getBytes());
			System.out.println(new String(name)+":"+new String(age));
		}
		
		table.close();
	}
	
	
	
	
}











