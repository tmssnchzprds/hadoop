package alojamientoTuristico;

import java.io.IOException;
import java.text.DecimalFormat;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class LocationMapperPorcentaje extends Mapper<Text, Text, Text, Text>{
	private final DecimalFormat decimalFormat = new DecimalFormat("#.##");
	private long total;
	public void setup(Context contexto) throws IOException, InterruptedException {
		total = contexto.getConfiguration().getLong("total", 0);
	}
	public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
		Double res = 100 * Double.parseDouble(value.toString())/total;
		context.write(key, new Text(decimalFormat.format(res)+"%"));
	}
}
