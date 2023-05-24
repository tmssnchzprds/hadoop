package alojamientoTuristico;

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

enum CANTIDAD {
	TOTAL
	}

public class LocationMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
	private static final String SEPARATOR = ";";
	private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd");
	private String pais;
	private String ciudad;
	private String distrito;
	public void setup(Context contexto) throws IOException, InterruptedException {
		pais = contexto.getConfiguration().get("pais", "" );
		ciudad = contexto.getConfiguration().get("ciudad", "" );
		distrito = contexto.getConfiguration().get("distrito", "" );
	}
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		/**
		 * "apartment_id";	"md5";			"name";			"description";		"host_id";	"neighborhood_overview";
		 * 36187629;		"66fff4...";	Piso refor...;	"Apartamen...";		261787331;	;

		 * "neighbourhood_name";	"neighbourhood_district";	"latitude";		"longitude";	"room_type";		
		 * BETERO;					POBLATS MARITIMS;			39.47149;		-0.3346;		Entire home/apt;

		 * "accommodates";		"bathrooms";	"bedrooms";		"beds";		"amenities_list";
		 * 4;					2;				3;				3;			"{TV,Wifi,""Air conditioning"",Refrigerator}";

		 * "price";	"minimum_nights";	"maximum_nights";	"has_availability";		"availability_30";		"availability_60";
		 * 90.0;	5;					20;					true;					0;						4;

		 * "availability_90";		"availability_365";		"number_of_reviews";		"first_review_date";		"last_review_date";
		 * 34;						34;						1;							2019-08-19;					2019-08-19;

		 * "review_scores_rating";		"review_scores_accuracy";		"review_scores_cleanliness";	"review_scores_checkin";
		 * 100.0;						10.0;							10.0;							10.0;

		 * "review_scores_communication";		"review_scores_location";		"review_scores_value";		"license";
		 * 10.0;								10.0;							10.0;						;

		 * "is_instant_bookable";		"reviews_per_month";	"country";	"city";		"insert_date"
		 * true;						0.22;					spain;		valencia;	2019-12-31
		 */
		final String[] values = value.toString().split(SEPARATOR);
		// final int apartment_id = NumberUtils.toInt(values[0]);
		// final String md5 = format(values[1]);
		// final String name = format(values[2]);
		// final String description = format(values[3]);
		// final int host_id = NumberUtils.toInt(values[4]);
		// final String neighborhood_overview = format(values[5]);
		final String neighbourhood_name = format(values[6]);
		final String neighbourhood_district = format(values[7]);
		// final long latitude = NumberUtils.toLong(values[8]);
		// final long longitude = NumberUtils.toLong(values[9]);
		// final String room_type = format(values[10]);
		// final int accommodates = NumberUtils.toInt(values[11]);
		// final int bathrooms = NumberUtils.toInt(values[12]);
		// final int bedrooms = NumberUtils.toInt(values[13]);
		// final int beds = NumberUtils.toInt(values[14]);
		// final String amenities_list = format(values[15]);
		// final double price = NumberUtils.toDouble(values[16]);
		// final int minimum_nights = NumberUtils.toInt(values[17]);
		// final int maximum_nights = NumberUtils.toInt(values[18]);
		// final boolean has_availability = Boolean.parseBoolean(values[19]);
		// final int availability_30 = NumberUtils.toInt(values[20]);
		// final int availability_60 = NumberUtils.toInt(values[21]);
		// final int availability_90 = NumberUtils.toInt(values[22]);
		// final int availability_365 = NumberUtils.toInt(values[23]);
		// final int number_of_reviews = NumberUtils.toInt(values[24]);
		// final Calendar first_review_date = Calendar.getInstance();
		// final Calendar last_review_date = Calendar.getInstance();	
		// final int review_scores_rating = NumberUtils.toInt(values[27]);		
		// final int review_scores_accuracy = NumberUtils.toInt(values[28]);
		// final int review_scores_cleanliness = NumberUtils.toInt(values[29]);
		// final int review_scores_checkin = NumberUtils.toInt(values[30]);
		// final int review_scores_communication = NumberUtils.toInt(values[31]);
		// final int review_scores_location = NumberUtils.toInt(values[32]);
		// final int review_scores_value = NumberUtils.toInt(values[33]);
		// final String license = format(values[34]);
		// final boolean is_instant_bookable = Boolean.parseBoolean(values[35]);
		// final double reviews_per_month = NumberUtils.toDouble(values[36]);
		final String country = format(values[37]);
		final String city = format(values[38]);
		// final Calendar insert_date = Calendar.getInstance();
		// try {
			// first_review_date.setTime(sdf.parse(values[25]));
			// last_review_date.setTime(sdf.parse(values[26]));
			// insert_date.setTime(sdf.parse(values[39]));
		// } catch (ParseException e) {
			// e.printStackTrace();
		// }
		
		if (!distrito.equals("")) {
			if (distrito.equals(neighbourhood_district.toString())) {
				context.getCounter(CANTIDAD.TOTAL).increment(1);
				context.write(new Text(neighbourhood_name.toString()), new IntWritable(1));
			}
		} else if (!ciudad.equals("")) {
			if (ciudad.equals(city.toString())) {
				context.getCounter(CANTIDAD.TOTAL).increment(1);
				context.write(new Text(neighbourhood_district.toString()), new IntWritable(1));
			}
		} else if (!pais.equals("")) {
			if (pais.equals(country.toString())) {
				context.getCounter(CANTIDAD.TOTAL).increment(1);
				context.write(new Text(city.toString()), new IntWritable(1));
			}
		} else {
			context.getCounter(CANTIDAD.TOTAL).increment(1);
			context.write(new Text(country.toString()), new IntWritable(1));
		}
	}
	private String format(String value) {
		return value.trim();
	}
}
