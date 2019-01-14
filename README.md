# Spark Application

## Getting Started
The recommend way of use is to build jar using maven plugin and run it with parameters using java command.

* Build project with maven :
```
mvn clean install
```
* Run jar with 1 parameter:
```
java -jar pathToJarFile.jar parameter1
pathToJarFile.jar: Path to generated jar file
parameter1: Path to csv file
```

## TaskUtil methods

def createRDD(sparkSession: SparkSession, path: String): RDD[Option[Hotel]]

/**
    * Creates RDD from a set of hotels, read from csv file
    *
    * @param sparkSession
    * @param path - path to csv file
    * @return rdd of Option[Hotel]
    */

 def filterNullHotelsRDD(hotelsRDD: RDD[Option[Hotel]]): RDD[Hotel]

/**
    * Creates RDD from a set of hotels, read from csv file
    *
    * @param sparkSession
    * @param path - path to csv file
    * @return rdd of Option[Hotel]
    */

def task1(notNullHotelsRDD: RDD[Hotel]): Array[((Int, Int, Int), Int)]

/**
    * Finds top 3 most popular hotels between couples(treats hotel as composite key of continent, country and market).
    *
    * @param hotelsRDD - input rdd
    * @return Array[((k1, k2, k3), count)], where (k1, k2, k3) is a composite key of continent, country and market
    *         and count is this hotel`s frequency in rdd.
    **/

def task2(notNullHotelsRDD: RDD[Hotel]): Array[(Int, Int)]

/**
    * Finds the most popular country where hotels are booked and searched from the same country
    *
    * @param hotelsRDD - input rdd
    * @return Array[(k1, count)] where k1 is hotel country and count is this hotel`s frequency in rdd.
    **/


def task3(notNullHotelsRDD: RDD[Hotel]): Array[((Int, Int, Int), Int)]

/**
    * Finds top 3 hotels where people with children are interested but not booked in the end
    *
    * @param hotelsRDD - input rdd
    * @return Array[((k1, k2, k3), count)], where (k1, k2, k3) is a composite key of continent, country and market
    *         and count is this hotel`s frequency in rdd.
    **/