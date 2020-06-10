package com.dudu.app.log.mr;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

public class AppLogDataClean {

	
	
	public static class ApplogDataCleanMapper extends Mapper<LongWritable, Text, Text, NullWritable>{
		
		Text k = null;
		NullWritable v = null;
		SimpleDateFormat sdf = null;
		MultipleOutputs<Text, NullWritable> mos = null;
		
		@Override
		protected void setup(Mapper<LongWritable, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			k = new Text();
			v = NullWritable.get();
			sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			mos = new MultipleOutputs<Text, NullWritable >(context);
		}
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			
			JSONObject parseObject = JSON.parseObject(value.toString());
			
			JSONObject jsonObject = parseObject.getJSONObject("header");
			
			if(StringUtils.isBlank(jsonObject.getString("sdk_ver"))) {
				return;
			}
			
			if(StringUtils.isBlank(jsonObject.getString("time_zone"))) {
				return;
			}
			
			if(StringUtils.isBlank(jsonObject.getString("commit_id"))) {
				return;
			}
			
			if(StringUtils.isBlank(jsonObject.getString("commit_time"))) {
				return;
			}
			String string = jsonObject.getString("commit_time");
			String format = sdf.format(new Date(Long.parseLong(string)+962*24*60*60*1000L));
			jsonObject.put("commit_time", format);
			
			if(StringUtils.isBlank(jsonObject.getString("pid"))) {
				return;
			}
			
			if(StringUtils.isBlank(jsonObject.getString("app_token"))) {
				return;
			}
			
			if(StringUtils.isBlank(jsonObject.getString("app_id"))) {
				return;
			}
			
			if(StringUtils.isBlank(jsonObject.getString("device_id")) || jsonObject.getString("device_id").length()<17) {
				return;
			}
			
			if(StringUtils.isBlank(jsonObject.getString("device_id_type"))) {
				return;
			}
			
			if(StringUtils.isBlank(jsonObject.getString("release_channel"))) {
				return;
			}
			
			if(StringUtils.isBlank(jsonObject.getString("app_ver_name"))) {
				return;
			}
			
			if(StringUtils.isBlank(jsonObject.getString("app_ver_code"))) {
				return;
			}
			
			if(StringUtils.isBlank(jsonObject.getString("os_ver"))) {
				return;
			}
			
			if(StringUtils.isBlank(jsonObject.getString("language"))) {
				return;
			}
			
			if(StringUtils.isBlank(jsonObject.getString("country"))) {
				return;
			}
			
			if(StringUtils.isBlank(jsonObject.getString("manufacture"))) {
				return;
			}
			
			if(StringUtils.isBlank(jsonObject.getString("device_model"))) {
				return;
			}
			
			if(StringUtils.isBlank(jsonObject.getString("resolution"))) {
				return;
			}
			
			if(StringUtils.isBlank(jsonObject.getString("net_type"))) {
				return;
			}
			/**
			 * 生成user_id
			 */
			String user_id = "";
			if(jsonObject.getString("os_name").trim().equals("android")) {
				user_id=StringUtils.isBlank(jsonObject.getString("android_id"))?jsonObject.getString("device_id"):jsonObject.getString("android_id");
			}else {
				user_id=jsonObject.getString("device_id");
			}
			jsonObject.put("user_id", user_id);
			
			/**
			 * 输出结果
			 */
			k.set(JsonToStringUtil.toString(jsonObject));
			if("android".equals(jsonObject.getString("os_name"))) {
				mos.write(k, v, "android/android");
			}else {
				mos.write(k, v, "ios/ios");
			}
			
		}
		
		@Override
		protected void cleanup(Mapper<LongWritable, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			mos.close();
		}
		
		
	}
	
	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(AppLogDataClean.class);
		
		job.setMapperClass(ApplogDataCleanMapper.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);
		
		job.setNumReduceTasks(0);
		
		LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		boolean res = job.waitForCompletion(true);
		System.out.println(res?0:1);
		
		
	}
	
	
	
	
	
	
	
}
