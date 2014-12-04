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
        
public class ReportCustTransInfo {
        
 public static class Map extends Mapper<LongWritable, Text, IntWritable, Text> {
 
    private IntWritable customerID = new IntWritable(0);
	private Text transactionInfo = new Text();
        
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String tag = null;
		
		int ID = 0;
		String name = null;
		float salary;
		float transTotal = 0;
		int minItems = 0;
		
        String line = value.toString();
		String[] splits = line.split(",");
		
		FileSplit fileSplit = (FileSplit)context.getInputSplit();
		String filename = fileSplit.getPath().getName();
		
		if(filename.contains("Customers")) {
			tag = "Customer";
			ID = Integer.parseInt(splits[0]);
			name = splits[1];
			salary = Float.parseFloat(splits[4]);
			customerID.set(ID);
			transactionInfo.set(tag + "," + String.valueOf(ID) + "," + name + "," + String.valueOf(salary));
			context.write(customerID, transactionInfo);
		}
		else{
			tag = "Transaction";
			ID = Integer.parseInt(splits[1]);
			transTotal = Float.parseFloat(splits[2]);
			minItems = Integer.parseInt(splits[3]);
			customerID.set(ID);
			transactionInfo.set(tag + "," + String.valueOf(ID) + "," + String.valueOf(transTotal) + "," + String.valueOf(minItems));
			context.write(customerID, transactionInfo);
		}
    }
 } 
 public static class Reduce extends Reducer<IntWritable, Text, IntWritable, Text>{
	
	java.util.Map<Integer, String> custTransInfo = new HashMap<Integer, String>();
	
	public void reduce(IntWritable key, Iterable<Text> values, Context context)throws IOException, InterruptedException {
	
		for(Text value : values){
			String line = value.toString();
			String[] splits = line.split(",");
			if(splits[0].equals("Customer")){
				if(!custTransInfo.containsKey(key.get())) {
					custTransInfo.put(key.get(), splits[1] + "," + splits[2] + "," + splits[3] + ",0,0,0");
				}
				else{
					String[] current = custTransInfo.get(key.get()).split(",");
					if(current[1].equals("") && current[2].equals("")){
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
					if(current[4].isEmpty()){
						System.err.println(value.toString());
					}
					final int count = Integer.parseInt(current[3]) + 1;
					final float currentTransTotal = Float.parseFloat(current[4]);
					final int currentMinItems = Integer.parseInt(current[5]);
					final int minItems = (Integer.parseInt(splits[3]) < currentMinItems)? Integer.parseInt(splits[3]) : currentMinItems;
					custTransInfo.put(key.get(), current[0] + "," + current[1] + "," + current[2] + "," + count + "," + String.valueOf(currentTransTotal + Float.parseFloat(splits[2])) + "," + minItems);
				}
			}
		}
		
	}

	public void cleanup(Context context) {
		try{
			ArrayList<java.util.Map.Entry<Integer, String>> entries = new ArrayList<java.util.Map.Entry<Integer, String>>(custTransInfo.entrySet());
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
	job.setJarByClass(ReportCustTransInfo.class);
	job.setJobName("ReportCustTransInfo");
    
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
