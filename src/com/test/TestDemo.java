package com.test;


import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.junit.Test;


public class TestDemo {
	
	@Test
	public void createTable() throws MasterNotRunningException, ZooKeeperConnectionException, IOException {
	
		//获取hbase的环境变量参数对象
		Configuration configuration = HBaseConfiguration.create();
		
		//设置zk集群链接地址，可以只写一个
		configuration.set("hbase.zookeeper.quorum", "hadoop01:2181,hadoop02:2181,hadoop03:2181");
		
		HBaseAdmin hBaseAdmin = new HBaseAdmin(configuration);
		
		//创建Hbase表对象并指定表名
		HTableDescriptor table = new HTableDescriptor(TableName.valueOf("tab2"));
		
		//创建列族对象
		HColumnDescriptor cf1 = new HColumnDescriptor("cf1");
		HColumnDescriptor cf2 = new HColumnDescriptor("cf2");
		
		//指定某个列族的cell最多保留的历史版本数，默认是3
		cf1.setMaxVersions(3);
		//将列族和表产生绑定关系
		table.addFamily(cf1);
		table.addFamily(cf2);
		
		hBaseAdmin.createTable(table);
		hBaseAdmin.close();
		System.out.println("test");
		
	}
	
	
	/**
	 * 插入数据
	 * @throws Exception
	 */
	@Test
	public void putData() throws Exception {
		//获取hbase的环境变量参数对象
		Configuration configuration = HBaseConfiguration.create();
				
		//设置zk集群链接地址，可以只写一个
		configuration.set("hbase.zookeeper.quorum", "hadoop01:2181,hadoop02:2181,hadoop03:2181");
		
		//创建要操作的表对象
		HTable table = new HTable(configuration, "tab1");
		Put row = new Put("row1".getBytes());
		
		//定义要插入的列族和列数据
		row.add("cf1".getBytes(),"name".getBytes(),"Tom".getBytes());
		row.add("cf1".getBytes(),"age".getBytes(),"23".getBytes());
		row.add("cf2".getBytes(),"gender".getBytes(),"man".getBytes());
		
		table.put(row);
		table.close();
	}
	
	
	/**
	 * 插入100行数据
	 * @throws Exception
	 */
	@Test
	public void put100Rows() throws Exception {
		//获取hbase的环境变量参数对象
		Configuration configuration = HBaseConfiguration.create();
				
		//设置zk集群链接地址，可以只写一个
		configuration.set("hbase.zookeeper.quorum", "hadoop01:2181,hadoop02:2181,hadoop03:2181");
		
		//创建要操作的表对象
		HTable table = new HTable(configuration, "tab2");
		
		for (int i = 1; i <= 100; i++) {
			Put row = new Put(("row"+i).getBytes());
			row.add("cf1".getBytes(),"number".getBytes(),(i+"").getBytes());
			table.put(row);
		}
		table.close();
	}
	
	/**
	 * 获取数据
	 * @throws Exception 
	 */
	@Test
	public void getData() throws Exception {
		//获取hbase的环境变量参数对象
		Configuration configuration = HBaseConfiguration.create();
				
		//设置zk集群链接地址，可以只写一个
		configuration.set("hbase.zookeeper.quorum", "hadoop01:2181,hadoop02:2181,hadoop03:2181");
		
		HTable table = new HTable(configuration, "tab1");
		Get get = new Get("row1".getBytes());
		//执行查询  返回结果集
		Result result = table.get(get);
		byte[] name=result.getValue("cf1".getBytes(), "name".getBytes());
		byte[] age=result.getValue("cf1".getBytes(), "age".getBytes());
		byte[] gender=result.getValue("cf2".getBytes(), "gender".getBytes());
		
		System.out.println(new String(name)+":"+new String(age)+","+new String(gender));
		table.close();
	}
	
	/**
	 * 扫描表
	 * @throws IOException 
	 */
	@Test
	public void scanTable() throws IOException {
		//获取hbase的环境变量参数对象
		Configuration configuration = HBaseConfiguration.create();
						
		//设置zk集群链接地址，可以只写一个
		configuration.set("hbase.zookeeper.quorum", "hadoop01:2181,hadoop02:2181,hadoop03:2181");
		
		HTable table = new HTable(configuration, "tab2");
		
		Scan scan = new Scan();
		ResultScanner resultScanner = table.getScanner(scan);
		
		Iterator<Result> iterator = resultScanner.iterator();
		
		while(iterator.hasNext()){
			Result res = iterator.next();
			byte[] num = res.getValue("cf1".getBytes(), "number".getBytes());
			System.out.println(new String(num));
		}
		table.close();
	}
	
	/**
	 * 删除行数据
	 * @throws IOException 
	 */
	@Test
	public void deleteRow() throws IOException {
		//获取hbase的环境变量参数对象
		Configuration configuration = HBaseConfiguration.create();
						
		//设置zk集群链接地址，可以只写一个
		configuration.set("hbase.zookeeper.quorum", "hadoop01:2181,hadoop02:2181,hadoop03:2181");
		
		HTable table = new HTable(configuration, "tab1");
		
		Delete delete = new Delete("row1".getBytes());
		
		table.delete(delete);
		table.close();
	}
	
	
	/**
	 * 删除表
	 * @throws IOException 
	 */
	@Test
	public void deleteTable() throws IOException {
		//获取hbase的环境变量参数对象
		Configuration configuration = HBaseConfiguration.create();
						
		//设置zk集群链接地址，可以只写一个
		configuration.set("hbase.zookeeper.quorum", "hadoop01:2181,hadoop02:2181,hadoop03:2181");
		
		HBaseAdmin admin = new HBaseAdmin(configuration);
		admin.disableTable("tab1".getBytes());
		admin.deleteTable("tab1".getBytes());

		admin.close();
	}
	
}
