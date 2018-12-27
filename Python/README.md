# Python Spark examples
Arrancamos la consola:

./bin/pyspark

## Ejemplo 1 (cuenta lineas con la palabra Spark)
readme_file = sc.textFile("README.md") 
spark_mentions = readme_file.filter(lambda line: "Spark" in line) 

//count the lines having Spark in them 
spark_mentions.count()

## Ejemplo 2 (retorna las diez palabras mas frecuentes, con el numero de ocurrencias)
readme_file = sc.textFile("README.md") 
//for each line tokenizes splitting using the spaces, and the result is returned as a interaction. For each iteraction creates a pair with the format word, 1
//Finally it reduces the pairs by key, that is, by word. The reduce does add up the values, so at the end we have a list of pairs with the word as key
//and the number of occurrences as value
counts = readme_file.flatMap(lambda line: line.split(" ")).map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b) 
//Returns the top ten
counts.takeOrdered(10, key=lambda x: -x[1])

## Ejemplo 3 (crea una collección de parejas)
//A Pythin array
pairs = [(1, 1), (1, 2), (2, 3), (2, 4), (3, 0)] 
//Creates a rdd pair based on the array
pairs_rdd = sc.parallelize(pairs) 
//Fetches the first five
pairs_rdd.take(5)

### Reduce por key
// 
pairs_rdd.reduceByKey(lambda a, b: a + b).take(5)
