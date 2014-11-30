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
        
 public static class Map extends Mapper<LongWritable, Text, Text, Text> {
 
    private Text customerName = new Text();
	private Text transactionInfo = new Text();
        
    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
	
        String line = value.toString();
		String[] splits = line.split(",");
		CustomerName.set(splits[1]);
		transactionInfo.set(splits[3] + "," + splits[2]);
		output.collect(customerName, transactionInfo);
    }
 } 
 public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text>{
 
	Hashtable<String, String> transInfo = new Hashtable<String, String>();
	
	public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter)throws IOException {
			int transNum;
			float transTotal;
			
			while (values.hasNext()) {
				String line = values.next().toString();
				String[] splits = line.split(",");

				transNum = Integer.parseInt(splits[0]);
				transTotal = Float.parseFloat(splits[1]);

				if(!transInfo.containsKey(key)) {
					transInfo.put(key, String.valueOf(transNum) + "," + String.valueOf(transTotal));
				}
				else{
					String[] currentValues = transInfo.get(key).split(",");
					final int currentTransNum = currentValues[0];
					final float currentTransTotal = currentValues[1];
					transInfo.put(currentKey, String.valueOf(transNum + currentTransNum) + "," + String.valueOf(transTotal + currentTransTotal));
				}
			}
	
			ArrayList<java.util.Map.Entry<String, String>> entries = new ArrayList(transInfo.entrySet());
			for(java.util.Map.Entry<String, String> entry: entries) {
				output.collect(new Text(entry.getKey()), new Text(entry.getValue()));	
			}
	}
 }
        
 public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration(ReportTransInfo.class);
        
    Job job = new Job(conf, "ReportTransInfo");
    
    job.setOutputKeyClass(Text.class);
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