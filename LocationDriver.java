package alojamientoTuristico;
// Clase principal de la aplicación que ejecuta dos trabajos encadenados
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configured;

public class LocationDriver extends Configured implements Tool {
        public int run(String[] args) throws Exception {
		if (args.length < 2) {
      			System.err.println("Uso: jobs <in> [<in>...] <out>");
			System.exit(2);
		}
		
		Job job = Job.getInstance(getConf());
		job.setJarByClass(getClass());

		job.setMapperClass(LocationMapper.class);
		job.setCombinerClass(LocationReducer.class);
		job.setReducerClass(LocationReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		for (int i = 0; i < args.length-1; ++i)
			FileInputFormat.addInputPath(job, new Path(args[i]));

		// se usa un directorio intermedio para que el primer
		// trabajo deje sus datos al segundo
		Path tmp = new Path(args[args.length-1] + "-tmp");
    		FileOutputFormat.setOutputPath(job, tmp);

    	// Se ejecuta el primero trabajo.
		if (!job.waitForCompletion(true))
			return 1;

		// Recuperamos el contador que hemos enviado tras ejecutar la primera tarea.
		long total = job.getCounters().findCounter(CANTIDAD.TOTAL).getValue();
		job = Job.getInstance(getConf());

		job.setJarByClass(getClass());

		// Para el segundo trabajo, indicamos que el Mapper va a recibir un tipo clave-valor texto.
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		job.setMapperClass(LocationMapperPorcentaje.class);
		// Este mapper no tiene reducer, por lo que indicamos como 0 el número de tareas de reducer.
		job.setNumReduceTasks(0);

		// Indicamos los tipos de salida.
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

		// Ahora sí indicamos el directorio de salida que recibimos por parámetro.
		FileInputFormat.addInputPath(job, tmp);
    		FileOutputFormat.setOutputPath(job, new Path(args[args.length-1]));

    	// Asignamos el parámetro num_palabras, con el valor que recuperamos del contadore del trabajo anterior.
		job.getConfiguration().setLong("total", total);

		// Ejecutamos la segunda tarea.
		return job.waitForCompletion(true) ? 0 : 1;
	}
        public static void main(String[] args) throws Exception {
                int resultado = ToolRunner.run(new LocationDriver(), args);
                System.exit(resultado);
        }
}