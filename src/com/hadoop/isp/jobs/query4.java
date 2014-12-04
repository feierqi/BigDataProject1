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
	private Map<Integer, Integer> customers = new HashMap<Integer, Integer>();
	private Map<Integer, Float> transactions = new HashMap<Integer, Float>();
        
    public void map(LongWritable key, Text value, OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException {
		String tag = null;
		
		int ID = 0;
		int countryCode = 0;
		float transTotal = 0;
		
        String line = value.toString();
		String[] splits = line.split(",");
		
		FileSplit fileSplit = (FileSplit)reporter.getInputSplit();
		String filename = fileSplit.getPath().getName();
		if(filename.equals("Customers")) {
			tag = "Customer";
			ID = Integer.parseInt(splits[0]);
			countryCode = splits[3];
			customers.put(ID, countryCode);
		}
		else{
			tag = "Transaction";
			ID = Integer.parseInt(splits[1]);
			transTotal = Float.parseFloat(splits[2]);
			transactions.put(ID, transTotal);
		}
		
		ArrayList<java.util.Map.Entry<Integer, Float>> entries = new ArrayList(transactions.entrySet());
		for(java.util.Map.Entry<Integer, Float> entry: entries) {
				output.collect(new IntWritable(customers.get(entry.getKey())), new Text(customers.get(entry.getKey()) + "," + entry.getValue()));	
		}
    }
 } 
 public static class Reduce extends MapReduceBase implements Reducer<IntWritable, Text, IntWritable, Text>{
	
	Map<Integer, String> countryInfo = new HashMap<Integer, String>();
	
	public void reduce(IntWritable key, Iterator<Text> values, OutputCollector<IntWritable, Text> output, Reporter reporter)throws IOException {
	
		while (values.hasNext()) {
			String line = values.next().toString();
			String[] splits = line.split(",");
			
			int countryCode = Integer.parseInt(splits[0]);
			float transTotal = Float.parseFloat(splits[1]);
			
			if(!countryInfo.containsKey(countryCode)) {
				countryInfo.put(countryCode, countryCode + "," + 1 + "," + transTotal + "," + transTotal);
			}
			else{
				String[] current = custTransInfo.get(key.get()).split(",");
				int count = Integer.parseInt(current[1]) + 1;
				final float currentMinTransTotal = Integer.parseInt(current[2]);
				final float currentMaxTransTotal = Integer.parseInt(current[3]);
				final int minTransTotal = (transTotal < currentMinTransTotal)? transTotal : currentMinTransTotal;
				final int maxTransTotal = (transTotal > currentMaxTransTotal)? transTotal : currentMaxTransTotal;
				countryInfo.put(countryCode, current[0] + "," + count + "," + minTransTotal + "," + maxTransTotal);
			}
		}
		
		ArrayList<java.util.Map.Entry<Integer, String>> entries = new ArrayList(countryInfo.entrySet());
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