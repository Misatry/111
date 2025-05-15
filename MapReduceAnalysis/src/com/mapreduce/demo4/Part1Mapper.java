package com.mapreduce.demo4;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
/*
* 不同脚本参与的动漫情况分析
* */
public class Part1Mapper extends Mapper<Object, Text, Text, IntWritable> {

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
    	 //不处理第一行数据
        if (key.toString().equals("0")) {
            return;
        } else {
        }
        String[] words = value.toString().split(",");
        if(words.length == 11) {
            String id = words[8];
            id = id.substring(1, id.length() - 1);

            for (String stars : id.split("、")) {
                if(stars.length()>1){    context.write(new Text(stars), new IntWritable(1));}

            }


        }
    }
    }
    	 

