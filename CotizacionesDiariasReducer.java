package CotizacionesDiarias;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CotizacionesDiariasReducer extends Reducer<Text, IntWritable, Text, Text> {
	private Text count = new Text();
	public void reduce(Text key, Iterable<IntWritable> Values, Context context) throws IOException, InterruptedException {
		int total = 0;

		for (IntWritable Value : Values) {
			total += Value.get();
		}

		count.set(Integer.toString(total));
		context.write(key, count);
	}
}