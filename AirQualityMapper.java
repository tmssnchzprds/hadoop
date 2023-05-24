package AirQuality;

import java.io.IOException;

import org.apache.commons.lang3.math.NumberUtils;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AirQualityMapper extends Mapper<Object, Text, Text, DoubleWritable> {
	 
	private static final String SEPARATOR = ";";

	/**
	 * DIA; CO (mg/m3);NO (ug/m3);NO2 (ug/m3);O3 (ug/m3);PM10 (ug/m3);SH2 (ug/m3);PM25 (ug/m3);PST (ug/m3);SO2 (ug/m3);PROVINCIA;ESTACIóN
	 * 01/01/1997; 1.2; 12; 33; 63; 56; ; ; ; 19 ;áVILA ;ávila
	 */
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		final String[] values = value.toString().split(SEPARATOR);

		// final String date = format(values[0]);
		// final String co = format(values[1]);
		// final String no = format(values[2]);
		// final String no2 = format(values[3]);
		// final String o3 = format(values[4]);
		// final String pm10 = format(values[5]);
		// final String sh2 = format(values[6]);
		// final String pm25 = format(values[7]);
		// final String pst = format(values[8]);
		final String so2 = format(values[9]);
		final String province = format(values[10]);
		// final String station = format(values[11]);

		if (NumberUtils.isNumber(so2.toString())) {
			context.write(new Text(province), new DoubleWritable(NumberUtils.toDouble(so2)));
		}
	}

	private String format(String value) {
		return value.trim();
	}
}