package com.mapreduce.demo2;

import Util.DBUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.log4j.BasicConfigurator;

public class Part1Runner {
	public static String driverClass = DBUtil.driverClass;
    public static String dbUrl = DBUtil.dbUrl;
    public static String userName = DBUtil.userName;
    public static String passwd = DBUtil.passwd;
    public static String tableName = "part2";
    public static String [] fields = {"name","value"};
   
    public static void main(String[] args) throws Exception{
    	BasicConfigurator.configure();
    	Configuration conf = new Configuration();
    	DBConfiguration.configureDB(conf,driverClass,dbUrl,userName,passwd);
    	
    	
    	
    	Job job = Job.getInstance(conf);
    	job.setJarByClass(Part1Runner.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapperClass(Part1Mapper.class);
        job.setReducerClass(Part1Reduce.class);
        FileInputFormat.setInputPaths(job,new Path("hdfs://192.168.1.100:9000/new_data.csv"));
        DBOutputFormat.setOutput(job,tableName,fields);
        
        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0:1);
        
	}
}
