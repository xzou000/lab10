from pyspark import SparkContext
from pyspark.sql import SQLContext
import datetime

def filterBike(pId, lines):
	import csv
	for row in csv.reader(lines):
		if (row[6] == 'Greenwich Ave & 8 Ave' and row[3].startswith('2015-02-01')):
			yield (row[3][:19])

def filterTaxi(pId, lines):
	if pId == 0:
		next(lines)
	import csv
	import pyproj
	proj = pyproj.Proj(init="epsg:2263", preserve_units=True)
	gLoc = proj(-74.00263761, 40.73901691)
	sqm = 1320 ** 2
	for row in csv.reader(lines):
		try:
			dropoff = proj(float(row[5]), float(row[4]))
		except:
			continue
		distance = (dropoff[0]-gLoc[0]) ** 2 + (dropoff[1]-gLoc[1]) ** 2
		if distance < sqm:
			yield row[1][:19]


if __name__ == "__main__":
	sc = SparkContext()
	sqlContext = SQLContext(sc)
	taxi = sc.textFile('/data/share/bdm/yellow.csv.gz')
	bike = sc.textFile('/data/share/bdm/citibike.csv')
	gbike = bike.mapPartitionsWithIndex(filterBike).cache()
	gTaxi = taxi.mapPartitionsWithIndex(filterTaxi).cache()
	lBikes = gbike.collect()
	lTaxis = gTaxi.collect()

	# Using RDD
	gAll = (gTaxi.map(lambda x: (x, 0)) + gbike.map(lambda x: (x, 1)))

	df = sqlContext.createDataFrame(gAll, ('time', 'event'))
	df1 = df.select(df['time'].cast('timestamp').cast('long').alias('epoch'),'event')
	df1.registerTempTable('gAll')

	import pyspark.sql.functions as sf
	import pyspark.sql.window as sw
	window = sw.Window.orderBy('epoch').rangeBetween(-600, 0)
	df2 = df1.select('event', (1-sf.min(df1['event']).over(window))
							 .alias('has_taxi')).filter(df1['event'] == 1).select(sf.sum(sf.col('has_taxi')))

	df2.rdd.saveAsTextFile("lab_11_result")
