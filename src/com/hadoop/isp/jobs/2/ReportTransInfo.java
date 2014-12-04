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
        
 public static class Map extends Mapper<LongWritable, Text, IntWritable, Text> {
 
    private IntWritable customerID = new IntWritable(0);
	private Text out = new Text();
        
	@Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	
        String line = value.toString();
		String[] splits = line.split(",");
		customerID.set(Integer.parseInt(splits[1]));
		out.set(splits[1] + "," + 1 + "," + splits[2]);
		context.write(customerID, out);
    }
 }
 
 public static class Reduce extends Reducer<IntWritable, Text, IntWritable, Text>{
 
	
	public void reduce(IntWritable key, Iterable<Text> values, Context context)throws IOException, InterruptedException {
			int count = 0;
			float transTotal = 0;
			String[] splits = null;
			
			for(Text value : values) {
				splits = value.toString().split(",");
				count += Integer.parseInt(splits[1]);
				transTotal += Float.parseFloat(splits[2]);
			}
			context.write(key, new Text(key.get() + "," + count + "," + transTotal));
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
	job.setMapOutputValueClass(Text.class);
    
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(Text.class);
        
    job.setMapperClass(Map.class);
    job.setCombinerClass(Reduce.class);
	job.setReducerClass(Reduce.class);
		
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
        
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
    System.exit(job.waitForCompletion(true) ? 0 : 1);
 }
        
}
