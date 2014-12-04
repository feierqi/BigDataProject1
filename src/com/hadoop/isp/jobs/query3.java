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
        
public class ReportCustTransInfo {
        
 public static class Map extends Mapper<LongWritable, Text, IntWritable, Text> {
 
    private IntWritable customerID = new IntWritable(0);
	private Text transactionInfo = new Text();
        
    public void map(LongWritable key, Text value, OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException {
		String tag = null;
		
		int ID = 0;
		String name = null;
		float salary = null;
		float transTotal = 0;
		int minItems = 0;
		
        String line = value.toString();
		String[] splits = line.split(",");
		
		FileSplit fileSplit = (FileSplit)reporter.getInputSplit();
		String filename = fileSplit.getPath().getName();
		
		if(filename.equals("Customers")) {
			tag = "Customer";
			ID = Integer.parseInt(splits[0]);
			name = splits[1];
			salary = Float.parseFloat(splits[4]);
			output.collect(customerID.set(ID), transactionInfo.set(tag + "," + String.valueOf(ID) + "," + name + "," + String.valueOf(salary)));
		}
		else{
			tag = "Transaction";
			ID = Integer.parseInt(splits[1]);
			transTotal = Float.parseFloat(splits[2]);
			minItems = Integer.parseInt(splits[3]);
			output.collect(customerID.set(ID), transactionInfo.set(tag + "," + String.valueOf(ID) + "," + String.valueOf(transTotal) + "," + String.valueOf(minItems)))
		}
    }
 } 
 public static class Reduce extends MapReduceBase implements Reducer<IntWritable, Text, IntWritable, Text>{
	
	Map<Integer, String> custTransInfo = new HashMap<Integer, String>();
	
	public void reduce(IntWritable key, Iterator<Text> values, OutputCollector<IntWritable, Text> output, Reporter reporter)throws IOException {
	
		while (values.hasNext()) {
			String line = values.next().toString();
			String[] splits = line.split(",");
			if(splits[0].equals("Customer")){
				if(!custTransInfo.containsKey(key.get())) {
					custTransInfo.put(key.get(), splits[1] + "," + splits[2] + "," + splits[3] + ",0,0.0,0");
				}
				else{
					String[] current = custTransInfo.get(key.get()).split(",");
					if(current[1].equals("") && current[2].equals(""){
						custTransInfo.put(key.get(), current[0] + "," + splits[2] + "," + splits[3] + "," + current[3] + "," + current[4] + "," + current[5]);
					}
				}
			}
			else{
				if(!custTransInfo.containsKey(key.get())) {
					custTransInfo.put(key.get(), splits[1] + "," + "" + "," + "" + "," + 1 + "," + splits[2] + "," + splits[3]);
				}
				else{
					String[] current = custTransInfo.get(key.get()).split(",");
					final int count = Integer.parseInt(current[3]) + 1;
					final float currentTransTotal = Float.parseFloat(current[4]);
					final int currentMinItems = Integer.parseInt(current[5]);
					final int minItems = (Integer.parseInt(splits[4]) < currentMinItems)? Integer.parseInt(splits[4]) : currentMinItems;
					custTransInfo.put(key.get(), current[0] + "," + current[1] + "," + current[2] + "," + count + "," + String.valueOf(currentTransTotal + Float.parseFloat(splits[2])) + "," + minItems);
				}
			}
		}
		
		ArrayList<java.util.Map.Entry<Integer, String>> entries = new ArrayList(custTransInfo.entrySet());
		for(java.util.Map.Entry<Integer, String> entry: entries) {
				output.collect(new IntWritable(entry.getKey()), new Text(entry.getValue()));	
		}
	}
 }
        
 public static void main(String[] args) throws Exception {
  	if(args.length != 3){
		System.err.println("Usage: ReportCustTransInfo <input1 path> <input2 path> <output path>");
		System.exit(-1);
	}
    Configuration conf = new Configuration(ReportCustTransInfo.class);
        
    Job job = new Job(conf, "ReportCustTransInfo");
    
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(Text.class);
        
    job.setMapperClass(Map.class);
    job.setCombinerClass(Reduce.class);
	job.setReducerClass(Reduce.class);
		
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
        
    FileInputFormat.addInputPath(job, new Path(args[0]));
	FileInputFormat.addInputPath(job, new Path(args[1]));
    FileOutputFormat.setOutputPath(job, new Path(args[2]));
        
    JobClient.runJob(conf);
 }
        
}