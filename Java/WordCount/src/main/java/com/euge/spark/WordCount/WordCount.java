package com.euge.spark.WordCount;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

/**
 * Hello world!
 *
 */
public class WordCount 
{
	public static void main( String[] args ) throws FileNotFoundException, UnsupportedEncodingException
	{
		if (args.length < 1) {
			System.err.println("Please provide a full path to the input files"); System.exit(0); 
		} 
		if (args.length < 2) { 
			System.err.println("Please provide a full path to the output file"); 
			System.exit(0); 
		}
		//cuenta(args[0], args[1],"Spark");
		cuenta(args[0], args[1]);

	}

	public static void cuenta(String fuente, String destino,String Texto) throws FileNotFoundException {
		final SparkConf conf = new SparkConf().setAppName("TextLinesCount").setMaster("local"); 
		final JavaSparkContext context = new JavaSparkContext(conf);

		final JavaRDD<String> inputFile = context.textFile(fuente); 

		final JavaRDD<String> sparkMentions = inputFile.filter(s -> s != null && s.contains(Texto)); 

		// count the lines having Spark in them 
		final PrintWriter writer = new PrintWriter(destino); 
		writer.println(sparkMentions.count()); 
		writer.close();

		context.close();
		return;
	}

	public static void cuenta(String fuente, String destino) throws FileNotFoundException {
		final SparkConf conf = new SparkConf().setAppName("TextLinesCount").setMaster("local"); 
		final JavaSparkContext context = new JavaSparkContext(conf);

		final JavaRDD<String> inputFile = context.textFile(fuente); 

		final JavaPairRDD<String, Integer> counts = inputFile
				//Cada fila la mapea a un iterator. El iterator esta formado por el resultado de la tokenización del valor de cada linea del archivo
				.flatMap(s -> Arrays.asList(s.split(" ")).iterator())
				//Cada uno de los valores asi tokenizados es mapeado a una dupla que tendra el valor como clave, e un 1 como valor
				.mapToPair(word -> new Tuple2<>(word, 1))
				//Con la dupla aplicamos una reducción. Todos los valores que tengan la misma clave, es decir la palabra, se fusionan
				//sumando el valor. De esta forma tendremos una lista de duplas con la palabra y numero de ocurrencias
				.reduceByKey((a, b) -> a + b);

		counts.saveAsTextFile(destino);

		context.close();
		return;
	}

	public static void cuentaHDFS(String fuente, String destino) throws FileNotFoundException {
		final SparkConf conf = new SparkConf().setAppName("TextLinesCount").setMaster("local"); 
		final JavaSparkContext context = new JavaSparkContext(conf);

		final JavaRDD<String> inputFile = context.textFile("hdfs://"+fuente); 

		final JavaPairRDD<String, Integer> counts = inputFile
				//Cada fila la mapea a un iterator. El iterator esta formado por el resultado de la tokenización del valor de cada linea del archivo
				.flatMap(s -> Arrays.asList(s.split(" ")).iterator())
				//Cada uno de los valores asi tokenizados es mapeado a una dupla que tendra el valor como clave, e un 1 como valor
				.mapToPair(word -> new Tuple2<>(word, 1))
				//Con la dupla aplicamos una reducción. Todos los valores que tengan la misma clave, es decir la palabra, se fusionan
				//sumando el valor. De esta forma tendremos una lista de duplas con la palabra y numero de ocurrencias
				.reduceByKey((a, b) -> a + b);

		counts.saveAsTextFile("hdfs://"+destino);

		context.close();
		return;
	}
}
