import java.io.IOException;
import java.lang.InterruptedException;
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
	private FloatWritable transactionTotal = new FloatWritable();
        
	@Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	
        String line = value.toString();
		String[] splits = line.split(",");
		customerID.set(Integer.parseInt(splits[1]));
		transactionTotal.set(Float.parseFloat(splits[2]));
		context.write(customerID, transactionTotal);
    }
 }
 
 public static class Reduce extends Reducer<IntWritable, FloatWritable, IntWritable, Text>{
 
	java.util.Map<Integer, String> transInfo = new HashMap<Integer, String>();
	
	@Override
	public void reduce(IntWritable key, Iterable<FloatWritable> values, Context context)throws IOException, InterruptedException {
			int count;
			float transTotal;
			
			for(FloatWritable value : values) {
				
				transTotal = value.get();
				
				if(!transInfo.containsKey(key.get())) {
					transInfo.put(key.get(), String.valueOf(1) + "," + String.valueOf(transTotal));
				}
				else{
					String[] currentValues = transInfo.get(key.get()).split(",");
					count = Integer.parseInt(currentValues[0]) + 1;
					final float currentTransTotal = Float.parseFloat(currentValues[1]) + transTotal;
					transInfo.put(key.get(), String.valueOf(count) + "," + String.valueOf(currentTransTotal));
				}
			}
	}

	@Override
	public void cleanup(Context context) {
		try{
			ArrayList<java.util.Map.Entry<Integer, String>> entries = new ArrayList<java.util.Map.Entry<Integer, String>>(transInfo.entrySet());
			for(java.util.Map.Entry<Integer, String> entry: entries) {
				context.write(new IntWritable(entry.getKey()), new Text(entry.getKey() + "," + entry.getValue()));	 
			}
		}catch (Exception e){
			System.err.println("cleanup function error");
			System.exit(-1);
		}
	}
 }
        
 public static void main(String[] args) throws Exception {
 	if(args.length != 2){
		System.err.println("Usage: ReportTransInfo <input path> <output path>");
		System.exit(-1);
	}
        
    Job job = new Job();
	job.setJarByClass(ReportTransInfo.class);
	job.setJobName("ReportTransInfo");

	job.setMapOutputKeyClass(IntWritable.class);
	job.setMapOutputValueClass(FloatWritable.class);
    
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(Text.class);
        
    job.setMapperClass(Map.class);
    //job.setCombinerClass(Reduce.class);
	job.setReducerClass(Reduce.class);
		
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
        
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
    System.exit(job.waitForCompletion(true) ? 0 : 1);
 }
        
}
