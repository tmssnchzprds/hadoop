package alojamientoTuristico;

import java.io.IOException;
import java.text.DecimalFormat;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PriceReducer extends Reducer<Text, DoubleWritable, Text, Text>{
	private final DecimalFormat decimalFormat = new DecimalFormat("#.##");
	public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
		double valueSum=0;
		int valueCon=0;
		for (DoubleWritable val : values) {
			valueSum+=val.get();
			valueCon++;
		}
		context.write(key, new Text(decimalFormat.format(valueSum/valueCon)+"€"));
	}
}