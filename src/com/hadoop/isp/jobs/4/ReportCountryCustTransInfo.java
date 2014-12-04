import java.io.IOException;
import java.lang.InterruptedException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
        
public class ReportCountryCustTransInfo {
        
 public static class Map extends Mapper<LongWritable, Text, IntWritable, Text> {
 
    private IntWritable customerID = new IntWritable(0);
	private Text outputInfo = new Text();

        
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String tag = null;
		int ID = 0;
		float transTotal = 0;

		String line = value.toString();
		String[] splits = line.split(",");

		FileSplit fileSplit = (FileSplit)context.getInputSplit();
		String filename = fileSplit.getPath().getName();

		if(filename.contains("Customers")) {
			tag = "Customer";
			ID = Integer.parseInt(splits[0]);
			customerID.set(ID);
			outputInfo.set(tag + "," + splits[3]);
			context.write(customerID, outputInfo);
		}
		else{
			tag = "Transaction";
			ID = Integer.parseInt(splits[1]);
			transTotal = Float.parseFloat(splits[2]);
			customerID.set(ID);
			outputInfo.set(tag + "," + transTotal);
			context.write(customerID, outputInfo);
		}
	}
 } 
 public static class Reduce extends Reducer<IntWritable, Text, IntWritable, Text>{
	
	java.util.Map<Integer, String> countryInfo = new HashMap<Integer, String>();
	java.util.Map<Integer, String> combinedCountryInfo = new HashMap<Integer, String>();
	
	public void reduce(IntWritable key, Iterable<Text> values, Context context)throws IOException, InterruptedException {
	
		for(Text value : values) {
				String line = value.toString();
				String[] splits = line.split(",");

				if(splits[0].equals("Customer")){
					if(!countryInfo.containsKey(key.get())) {
						countryInfo.put(key.get(), splits[1] + "," + Float.MAX_VALUE + "," + Float.MIN_VALUE);
					}
					else{
						String[] current = countryInfo.get(key.get()).split(",");
						countryInfo.put(key.get(), splits[1] + "," + current[1] + "," + current[2]);
					}
				}
				else{
					if(!countryInfo.containsKey(key.get())) {
						countryInfo.put(key.get(), "" + "," + splits[1] + "," + splits[1]);
					}
					else{
						String[] current = countryInfo.get(key.get()).split(",");
						final float transTotal = Float.parseFloat(splits[1]);
						final float currentMinTransTotal = Float.parseFloat(current[1]);
						final float currentMaxTransTotal = Float.parseFloat(current[2]);
						final float newMin = (transTotal < currentMinTransTotal)? transTotal : currentMinTransTotal;
						final float newMax = (transTotal > currentMinTransTotal)? transTotal : currentMaxTransTotal;
						countryInfo.put(key.get(), current[0] + "," + newMin + "," + newMax);
					}
				}
		}

	}

	private void combineCountryCode() {
		ArrayList<java.util.Map.Entry<Integer, String>> entries = new ArrayList<java.util.Map.Entry<Integer, String>>(countryInfo.entrySet());
		for(java.util.Map.Entry<Integer, String> entry: entries) {
			String[] splits = entry.getValue().split(",");
			final int countryCode = Integer.parseInt(splits[0]);
			if(!combinedCountryInfo.containsKey(countryCode)) {
				combinedCountryInfo.put(countryCode, countryCode + "," + 1 + "," + splits[1] + "," + splits[2]);
			}
			else{
				String[] current = combinedCountryInfo.get(countryCode).split(",");
				int count = Integer.parseInt(current[1]) + 1;
				final float minTransTotal = Float.parseFloat(splits[1]);
				final float maxTransTotal = Float.parseFloat(splits[2]);
				final float currentMinTransTotal = Float.parseFloat(current[2]);
				final float currentMaxTransTotal = Float.parseFloat(current[3]);
				final float newMin = (minTransTotal < currentMinTransTotal)? minTransTotal : currentMinTransTotal;
				final float newMax = (maxTransTotal > currentMinTransTotal)? maxTransTotal : currentMaxTransTotal;
				combinedCountryInfo.put(countryCode, countryCode + "," + count + "," + newMin + "," + newMax);
			}
		}
	}
		

	public void cleanup(Context context){
		try{
			combineCountryCode();
			ArrayList<java.util.Map.Entry<Integer, String>> entries = new ArrayList<java.util.Map.Entry<Integer, String>>(combinedCountryInfo.entrySet());
			for(java.util.Map.Entry<Integer, String> entry: entries) {
					context.write(new IntWritable(entry.getKey()), new Text(entry.getValue()));	
			}
		}catch (Exception e){
			System.err.println("cleanup function error");
			System.exit(-1);
		}
	}
 }
        
 public static void main(String[] args) throws Exception {
   	if(args.length != 3){
		System.err.println("Usage: ReportCustTransInfo <input1 path> <input2 path> <output path>");
		System.exit(-1);
	}
    
    Job job = new Job();
	job.setJarByClass(ReportCountryCustTransInfo.class);
	job.setJobName("ReportCountryCustTransInfo");

    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(Text.class);
        
    job.setMapperClass(Map.class);
	job.setReducerClass(Reduce.class);
		
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
        
    FileInputFormat.addInputPath(job, new Path(args[0]));
	FileInputFormat.addInputPath(job, new Path(args[1]));
    FileOutputFormat.setOutputPath(job, new Path(args[2]));
        
    System.exit(job.waitForCompletion(true) ? 0 : 1);
 }
        
}
