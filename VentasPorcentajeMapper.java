package VentasPorcentaje;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class VentasPorcentajeMapper extends Mapper<Text, Text, Text, DoubleWritable>{

	private long total;
	public void setup(Context contexto) throws IOException, InterruptedException {
		total = contexto.getConfiguration().getLong("total", 0);
	}
	public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
		Double res = 100 * Double.parseDouble(value.toString())/total;
		context.write(key, new DoubleWritable(res));
	}
}
