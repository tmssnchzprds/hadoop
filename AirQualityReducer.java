package AirQuality;

import java.io.IOException;
import java.text.DecimalFormat;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AirQualityReducer extends Reducer<Text, DoubleWritable, Text, Text> {
	 
	private final DecimalFormat decimalFormat = new DecimalFormat("#.##");

	public void reduce(Text key, Iterable<DoubleWritable> so2Values, Context context) throws IOException, InterruptedException {
		int measures = 0;
		double totalso2 = 0.0f;

		for (DoubleWritable so2Value : so2Values) {
			totalso2 += so2Value.get();
			measures++;
		}

		if (measures > 0) {
			context.write(key, new Text(decimalFormat.format(totalso2 / measures)));
		}
	}
}