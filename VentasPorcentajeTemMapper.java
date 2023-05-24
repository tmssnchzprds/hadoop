package VentasPorcentaje;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import org.apache.commons.lang3.math.NumberUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

enum VENTAS {
	TOTALES
	}

public class VentasPorcentajeTemMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
	private static final String SEPARATOR = ";";
	/**
	 * InvoiceNo;	StockCode;	Description;			Quantity;	InvoiceDate;		UnitPrice;	CustomerID;		Country
	 * 536944;		22383;		LUNCH BAG SUKI DESIGN;	70;			03/12/2010 12:20;	1.65;		12557;			Spain
	 */
	private int mes;
	private int anio;
	public void setup(Context contexto) throws IOException, InterruptedException {
		mes = contexto.getConfiguration().getInt("mes", 0 );
		anio = contexto.getConfiguration().getInt("anio", 0 );
	}
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		final String[] values = value.toString().split(SEPARATOR);
		final SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy HH:mm");
		// final int invoiceNo = NumberUtils.toInt(values[0]);
		// final int stockCode = NumberUtils.toInt(values[1]);
		// final String description = format(values[2]);
		final int quantity = NumberUtils.toInt(values[3]);
		final Calendar invoiceDate = Calendar.getInstance();
		// final double unitPrice = NumberUtils.toDouble(values[5]);
		final String customerID = format(values[6]);
		// final String country = format(values[7]);
		try {
			invoiceDate.setTime(sdf.parse(values[4]));
		} catch (ParseException e) {
			e.printStackTrace();
		}
		context.getCounter(VENTAS.TOTALES).increment(quantity);
		
		if ((mes == (invoiceDate.get(Calendar.MONTH)+1)  || mes == 0) &&
			(anio == invoiceDate.get(Calendar.YEAR) || anio == 0)) {
			context.write(new Text(customerID.toString()), new IntWritable(quantity));
		}
		
	}
	private String format(String value) {
		return value.trim();
	}
}
