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
        
public class ReportCountryCustTransInfo {
        
 public static class Map extends Mapper<LongWritable, Text, IntWritable, Text> {
 
    private IntWritable customerID = new IntWritable(0);
	private Text outputInfo = new Text();
        
    public void map(LongWritable key, Text value, OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException {
		String tag = null;
		
		int ID = 0;
		int countryCode = 0;
		float transTotal = 0;
		
        String line = value.toString();
		String[] splits = line.split(",");
		
		FileSplit fileSplit = (FileSplit)reporter.getInputSplit();
		String filename = fileSplit.getPath().getName();
		if(filename.equals("customers")) {
			tag = "Customer";
			ID = Integer.parseInt(splits[0]);
			countryCode = splits[3];
			output.collect(customerID.set(ID), outputInfo.set(tag + "," + countryCode));
		}
		else{
			tag = "Transaction";
			ID = Integer.parseInt(splits[1]);
			transTotal = Float.parseFloat(splits[2]);
			output.collect(customerID.set(ID), outputInfo.set(tag + "," + transTotal));
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
					custTransInfo.put(key.get(), splits[1] + "," + 1 + "," + "0.0" + "," + Float.MAX_VALUE + "," + Float.MIN_VALUE);
				}
				else{
					String[] current = custTransInfo.get(key.get()).split(",");
					int count = Integer.parseInt(current[1]) + 1;
					ustTransInfo.put(key.get(), splits[1] + "," + count + "," + current[2] + "," + current[3]);
				}
			}
			else{
				if(!custTransInfo.containsKey(key.get())) {
					custTransInfo.put(key.get(), "" + "," + 1 + splits[1] + "," + splits[1] + "," + splits[1]);
				}
				else{
					String[] current = custTransInfo.get(key.get()).split(",");
					int count = Integer.parseInt(current[1]) + 1;
					final float currentTransTotal = Integer.parseInt(current[2]);
					final float newTransTotal = Float.parseFloat(splits[1]) + currentTransTotal;
					final float currentMinTransTotal = Float.parseFloat(current[3]);
					final float currentMaxTransTotal = Float.parseFloat(current[4]);
					final int currentMinItems = Integer.parseInt(current[5]);
					final int minTransTotal = (newTransTotal < currentMinTransTotal)? newTransTotal : currentMinTransTotal;
					final int maxTransTotal = (newTransTotal > currentMaxTransTotal)? newTransTotal : currentMaxTransTotal;
					custTransInfo.put(key.get(), current[0] + "," + count + "," + newTransTotal + "," + minTransTotal + "," + maxTransTotal);
				}
			}
		}
		
		ArrayList<java.util.Map.Entry<Integer, String>> entries = new ArrayList(custTransInfo.entrySet());
		for(java.util.Map.Entry<Integer, String> entry: entries) {
				String[] values = entry.getValue().split(",");
				String selectedValue = values[0] + "," + values[1] + "," + values[3] + "," + values[4];
				output.collect(new IntWritable(entry.getKey()), new Text(selectedValue));	
		}
	}
 }
        
 public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration(ReportCountryCustTransInfo.class);
        
    Job job = new Job(conf, "ReportCountryCustTransInfo");
    
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