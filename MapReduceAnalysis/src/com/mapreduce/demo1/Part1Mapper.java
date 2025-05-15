package com.mapreduce.demo1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
/*
* 不同类型动漫分析
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
        if(words.length == 11){
            String id = words[2];

                context.write(new Text(id), new IntWritable(1));

        }


    }
    }
    	 

