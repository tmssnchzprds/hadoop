package SalesCountry;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.StringTokenizer;

import org.apache.commons.lang3.math.NumberUtils;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
enum VENTAS {
	MAS_5000, ENTRE_3000_Y_5000, MENOS_3000
}
public class SalesCountryMapper extends Mapper<Object, Text, Text, DoubleWritable> {
	private static final String SEPARATOR = ",";
	/**
	 * Transaction_date,	Product,	Price,	Payment_Type,	Name,		City,		State,		Country,			Account_Created,	Last_Login,		Latitude,	Longitude
	 * 1/2/09 6:17,			Product1,	1200,	Mastercard,		carolina,	Basildon,	England,	United Kingdom,		1/2/09 6:00,		1/2/09 6:08,	51.5,		-1.1166667
	 */
	private String pais;
	public void setup(Context contexto) throws IOException, InterruptedException {
		pais = contexto.getConfiguration().get("pais","United States" );
	}
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		final String[] values = value.toString().split(SEPARATOR);
		// final String transaction_date = format(values[0]);
		// final String product = format(values[1]);
		final double price = NumberUtils.toDouble(values[2]);
		// final String payment_Type = format(values[3]);
		// final String name = format(values[4]);
		// final String city = format(values[5]);
		final String state = format(values[6]);
		final String country = format(values[7]);
		// final String account_Created = format(values[8]);
		// final String last_Login = format(values[9]);
		// final String latitude = format(values[10]);
		// final String longitude = format(values[11]);
		
		if (country.equals(pais)) {
			if (price > 5000) {
				context.getCounter(VENTAS.MAS_5000).increment(1);
			} else if (price < 3000) {
				context.getCounter(VENTAS.MENOS_3000).increment(1);
			} else {
				context.getCounter(VENTAS.ENTRE_3000_Y_5000).increment(1);
			}
			context.write(new Text(state), new DoubleWritable(price));
		}
	}

	private String format(String value) {
		return value.trim();
	}
}