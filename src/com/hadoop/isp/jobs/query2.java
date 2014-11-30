package com.hadoop.isp;
        
import java.io.IOException;
import java.util.*;
        
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
        
public class ReportTransInfo {
        
 public static class Map extends Mapper<LongWritable, Text, IntWritable, FloatWritable> {
 
    private IntWritable customerID = new IntWritable(0);
	private FloatWritable transactionTotal = new FloatWritable(0.0);
        
    public void map(LongWritable key, Text value, OutputCollector<IntWritable, FloatWritable> output, Reporter reporter) throws IOException {
	
        String line = value.toString();
		String[] splits = line.split(",");
		customerID.set(Integer.parseInt(splits[1]));
		transactionTotal.set(Float.parseFloat(splits[2]));
		output.collect(customerID, transactionTotal);
    }
 } 
 public static class Reduce extends MapReduceBase implements Reducer<IntWritable, FloatWritable, IntWritable, Text>{
 
	Hashtable<Integer, String> transInfo = new Hashtable<Integer, String>();
	
	public void reduce(IntWritable key, Iterator<FloatWritable> values, OutputCollector<IntWritable, Text> output, Reporter reporter)throws IOException {
			int count;
			float transTotal;
			
			while (values.hasNext()) {
				
				transTotal = values.next().get();
				
				if(!transInfo.containsKey(key.get())) {
					transInfo.put(key.get(), String.valueOf(1) + "," + String.valueOf(transTotal));
				}
				else{
					String[] currentValues = transInfo.get(key).split(",");
					count = Integer.parseInt(currentValues[0]) + 1;
					final float currentTransTotal = Float.parseFloat(currentValues[1]) + transTotal;
					transInfo.put(key.get(), String.valueOf(count) + "," + String.valueOf(currentTransTotal));
				}
			}
	
			ArrayList<java.util.Map.Entry<Integer, String>> entries = new ArrayList(transInfo.entrySet());
			for(java.util.Map.Entry<Integer, String> entry: entries) {
				output.collect(new IntWritable(entry.getKey()), new Text(entry.getKey() + "," + entry.getValue()));	 
			}
	}
 }
        
 public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration(ReportTransInfo.class);
        
    Job job = new Job(conf, "ReportTransInfo");
    
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(Text.class);
        
    job.setMapperClass(Map.class);
    job.setCombinerClass(Reduce.class);
	job.setReducerClass(Reduce.class);
		
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
        
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
    JobClient.runJob(conf);
 }
        
}