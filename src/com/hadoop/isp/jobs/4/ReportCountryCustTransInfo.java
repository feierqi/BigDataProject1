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
 
	private BufferedReader brReader;
	private java.util.Map<Integer, Integer> customers = new HashMap<Integer, Integer>();

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

		ID = Integer.parseInt(splits[1]);
		context.write(new IntWritable(customers.get(ID)), new Text(customers.get(ID) + "," + 1 + "," + splits[2]));	
    }
 } 
 public static class Reduce extends Reducer<IntWritable, Text, IntWritable, Text>{
	
	public void reduce(IntWritable key, Iterable<Text> values, Context context)throws IOException, InterruptedException {
		int count = 0;
		float minTransTotal = Float.MAX_VALUE;
		float maxTransTotal = Float.MIN_VALUE;
	
		for(Text value : values) {
			String line = value.toString();
			String[] splits = line.split(",");
			
			int countryCode = Integer.parseInt(splits[0]);
			float transTotal = Float.parseFloat(splits[2]);
			
			count += Integer.parseInt(splits[1]);
			minTransTotal = (transTotal < minTransTotal)? transTotal : minTransTotal;
			maxTransTotal = (transTotal > maxTransTotal)? transTotal : maxTransTotal;
		}
		context.write(null, new Text(key.get() + "," + count + "," + minTransTotal + "," + maxTransTotal));
	}
 }
        
 public static void main(String[] args) throws Exception {
   	if(args.length != 2){
		System.err.println("Usage: ReportCustTransInfo <input path> output path>");
		System.exit(-1);
	}

	final String NAME_NODE = "hdfs://localhost:3351";
    
    Job job = new Job();
	job.setJarByClass(ReportCountryCustTransInfo.class);
	job.setJobName("ReportCountryCustTransInfo");

	DistributedCache.addCacheFile(new URI(NAME_NODE + "/tmp/Customers.txt"), job.getConfiguration());

    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(Text.class);
        
    job.setMapperClass(Map.class);
	job.setReducerClass(Reduce.class);
		
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
        
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
    System.exit(job.waitForCompletion(true) ? 0 : 1);
 }
        
}
