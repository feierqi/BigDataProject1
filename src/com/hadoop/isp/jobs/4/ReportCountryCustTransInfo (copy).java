import java.io.IOException;
import java.lang.InterruptedException;
import java.util.*;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader; 
import java.net.URI;

import org.apache.hadoop.filecache.DistributedCache;
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
	private BufferedReader brReader;
	private java.util.Map<Integer, Integer> customers = new HashMap<Integer, Integer>();
	private List<String> transactions = new ArrayList<String>();

	public void setup(Context context) throws IOException, InterruptedException {
 
		Path[] cacheFilesLocal = DistributedCache.getLocalCacheFiles(context.getConfiguration());
 
		for (Path eachPath : cacheFilesLocal) {
			String strLineRead = "";
 
			try {
				brReader = new BufferedReader(new FileReader(eachPath.toString()));
 
				// Read each line, split and load to HashMap
				while ((strLineRead = brReader.readLine()) != null) {
					String[] splits = strLineRead.split(",");
					int ID = Integer.parseInt(splits[0]);
					int countryCode = Integer.parseInt(splits[3]);
					customers.put(ID, countryCode);
				}
			}catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}finally {
				if (brReader != null) {
					brReader.close();
				}
 
			}
 
		} 
 
	} 
        
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		int ID = 0;
		
        String line = value.toString();
		String[] splits = line.split(",");
		
		FileSplit fileSplit = (FileSplit)context.getInputSplit();
		String filename = fileSplit.getPath().getName();
		if(filename.contains("Customers")) {
		}
		else{
			transactions.add(splits[1] + "," + splits[2]);
		}
		
		for(String transaction: transactions) {
			String[] entries = transaction.split(",");
			ID = Integer.parseInt(entries[0]);
			context.write(new IntWritable(customers.get(ID)), new Text(customers.get(ID) + "," + entries[1]));	
		}
    }
 } 
 public static class Reduce extends Reducer<IntWritable, Text, IntWritable, Text>{
	
	java.util.Map<Integer, String> countryInfo = new HashMap<Integer, String>();
	
	public void reduce(IntWritable key, Iterable<Text> values, Context context)throws IOException, InterruptedException {
	
		for(Text value : values) {
			String line = value.toString();
			String[] splits = line.split(",");
			
			int countryCode = Integer.parseInt(splits[0]);
			float transTotal = Float.parseFloat(splits[1]);
			
			if(!countryInfo.containsKey(countryCode)) {
				countryInfo.put(countryCode, countryCode + "," + 1 + "," + transTotal + "," + transTotal);
			}
			else{
				String[] current = countryInfo.get(key.get()).split(",");
				int count = Integer.parseInt(current[1]) + 1;
				final float currentMinTransTotal = Integer.parseInt(current[2]);
				final float currentMaxTransTotal = Integer.parseInt(current[3]);
				final float minTransTotal = (transTotal < currentMinTransTotal)? transTotal : currentMinTransTotal;
				final float maxTransTotal = (transTotal > currentMaxTransTotal)? transTotal : currentMaxTransTotal;
				countryInfo.put(countryCode, current[0] + "," + count + "," + minTransTotal + "," + maxTransTotal);
			}
		}
	}

	public void cleanup(Context context){
		try{
			ArrayList<java.util.Map.Entry<Integer, String>> entries = new ArrayList<java.util.Map.Entry<Integer, String>>(countryInfo.entrySet());
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

	DistributedCache.addCacheFile(new URI("/tmp/Customers.txt"), job.getConfiguration());

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
