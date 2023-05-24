package VentasCliente;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.*;
import java.time.format.*;
import java.util.Calendar;
import java.util.Locale;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

enum VENTAS {
	ENERO, FEBRERO, MARZO, ABRIL, MAYO, JUNIO, JULIO, AGOSTO, SEPTIEMBRE, OCTUBRE, NOVIEMBRE, DICIEMBRE
}
public class VentasClienteMapper extends Mapper<Object, Text, Text, DoubleWritable> {
	private static final String SEPARATOR = ";";
	/**
	 * InvoiceNo;	StockCode;	Description;			Quantity;	InvoiceDate;		UnitPrice;	CustomerID;		Country
	 * 536944;		22383;		LUNCH BAG SUKI DESIGN;	70;			03/12/2010 12:20;	1.65;		12557;			Spain
	 */
	private int idCliente;
	public void setup(Context contexto) throws IOException, InterruptedException {
		idCliente = contexto.getConfiguration().getInt("idCliente", 0 );
	}
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		final String[] values = value.toString().split(SEPARATOR);
		final SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy HH:mm");
		// final int invoiceNo = NumberUtils.toInt(values[0]);
		// final int stockCode = NumberUtils.toInt(values[1]);
		// final String description = format(values[2]);
		final int quantity = NumberUtils.toInt(values[3]);
		final Calendar invoiceDate = Calendar.getInstance();
		final double unitPrice = NumberUtils.toDouble(values[5]);
		final int customerID = NumberUtils.toInt(values[6]);
		// final String country = format(values[7]);
		try {
			invoiceDate.setTime(sdf.parse(values[4]));
		} catch (ParseException e) {
			e.printStackTrace();
		}
		if (idCliente == customerID || idCliente == 0) {
			switch (invoiceDate.get(Calendar.MONTH)) { 
		    	case 0:
					context.getCounter(VENTAS.ENERO).increment(1);
		    		break;
		    	case 1:
					context.getCounter(VENTAS.FEBRERO).increment(1);
		    		break;
		    	case 2:
					context.getCounter(VENTAS.MARZO).increment(1);
		    		break;
		    	case 3:
					context.getCounter(VENTAS.ABRIL).increment(1);
		    		break;
		    	case 4:
					context.getCounter(VENTAS.MAYO).increment(1);
		    		break;
		    	case 5:
					context.getCounter(VENTAS.JUNIO).increment(1);
		    		break;
		    	case 6:
					context.getCounter(VENTAS.JULIO).increment(1);
		    		break;
		    	case 7:
					context.getCounter(VENTAS.AGOSTO).increment(1);
		    		break;
		    	case 8:
					context.getCounter(VENTAS.SEPTIEMBRE).increment(1);
		    		break;
		    	case 9:
					context.getCounter(VENTAS.OCTUBRE).increment(1);
		    		break;
		    	case 10:
					context.getCounter(VENTAS.NOVIEMBRE).increment(1);
		    		break;
		    	case 11:
					context.getCounter(VENTAS.DICIEMBRE).increment(1);
		    		break;
			}
			context.write(new Text(obtenerMes(invoiceDate)), new DoubleWritable(quantity*unitPrice));
		}
	}
	private String obtenerMes(Calendar fecha) {
	    Month mes = LocalDate.from(Instant.ofEpochMilli(fecha.getTimeInMillis()).atZone(ZoneId.systemDefault()).toLocalDate()).getMonth();
	    return mes.getDisplayName(TextStyle.FULL, new Locale("es", "ES")).toUpperCase();
	}
	private String format(String value) {
		return value.trim();
	}
}