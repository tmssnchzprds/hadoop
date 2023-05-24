package SalesCountry;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class SalesCountryDriver extends Configured implements Tool {
 
	@Override
	public int run(String[] args) throws Exception {
 
		if (args.length != 2) {
			System.err.println("CotizacionesDiarias required params: {input file} {output dir}");
			System.exit(-1);
		}
 
		deleteOutputFileIfExists(args);
 
		final Job job = new Job(getConf());
		job.setJarByClass(SalesCountryDriver.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
 
		job.setMapperClass(SalesCountryMapper.class);
		job.setReducerClass(SalesCountryReducer.class);
 
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
 
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
 
		job.waitForCompletion(true);
 
		return 0;
	}
 
	private void deleteOutputFileIfExists(String[] args) throws IOException {
		final Path output = new Path(args[1]);
		FileSystem.get(output.toUri(), getConf()).delete(output, true);
	}
 
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new SalesCountryDriver(), args);
	}
}