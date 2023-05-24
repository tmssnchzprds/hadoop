package CotizacionesDiarias;

import java.io.IOException;
import java.text.DecimalFormat;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CotizacionesDiariasMapper extends Mapper<Object, Text, Text, IntWritable> {
	private static final String SEPARATOR = ";";
	/**
	 * DIAYHORA; EMPRESA ; COTIZACIONACTUAL ; COTIZAZIONANTERIOR
	 * 20/01/2021 11:54:34;SANTANDER;4,54;4,49
	 */
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		final String[] values = value.toString().split(SEPARATOR);
		// final String datetime = format(values[0]);
		final String e = format(values[1]);
		final double cac = Double.parseDouble(values[2].replace(',', '.'));
		final double can = Double.parseDouble(values[3].replace(',', '.'));

		if (cac > can) {
			context.write(new Text(e), new IntWritable(1));
		}
	}

	private String format(String value) {
		return value.trim();
	}
}