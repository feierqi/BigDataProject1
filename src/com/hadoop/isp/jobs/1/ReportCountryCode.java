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
        
public class ReportCountryCode {
        
 public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text customerName = new Text();
        
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
		String[] splits = line.split(",");
		final int countryCode = Integer.parseInt(splits[3]);
		if(countryCode >= 2 && countryCode <= 6){
			customerName.set(splits[1]);
			context.write(customerName, one);
		}
    }
 } 
        
 public static void main(String[] args) throws Exception {
	if(args.length != 2){
		System.err.println("Usage: ReportCountryCode <input path> <output path>");
		System.exit(-1);
	}
    //Configuration conf = new Configuration(ReportCountryCode.class);
        
    Job job = new Job();
	job.setJarByClass(ReportCountryCode.class);
	job.setJobName("Report Country Code");
    
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
        
    job.setMapperClass(Map.class);
        
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
        
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
    System.exit(job.waitForCompletion(true) ? 0 : 1);
 }
        
}
