package alojamientoTuristico;
import org.apache.hadoop.fs.FileSystem;
// Clase principal de la aplicación que ejecuta dos trabajos encadenados
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;

public class PriceDriver extends Configured implements Tool {
        public int run(String[] args) throws Exception {
		if (args.length < 2) {
      			System.err.println("Uso: jobs <in> [<in>...] <out>");
			System.exit(2);
		}
		 
		deleteOutputFileIfExists(args);
 
		final Job job = new Job(getConf());
		job.setJarByClass(PriceDriver.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
 
		job.setMapperClass(PriceMapper.class);
		job.setReducerClass(PriceReducer.class);
 
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
		ToolRunner.run(new PriceDriver(), args);
	}
}