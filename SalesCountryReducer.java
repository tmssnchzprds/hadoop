package SalesCountry;

import java.io.IOException;
import java.text.DecimalFormat;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SalesCountryReducer extends Reducer<Text, DoubleWritable, Text, Text> {
	private final DecimalFormat decimalFormat = new DecimalFormat("#.##");

	private Text count = new Text();
	public void reduce(Text key, Iterable<DoubleWritable> Values, Context context) throws IOException, InterruptedException {
		double total = 0;

		for (DoubleWritable Value : Values) {
			total += Value.get();
		}
		context.write(key, new Text(decimalFormat.format(total)));
	}
}