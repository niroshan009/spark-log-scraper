# spark-log-scraper

## set up the sqlContext
assign sql context to variable

`val sqlContext = new org.apache.spark.sql.SQLContext(sc)`

## read logs to a variable
Read csv files to variable

`val item = spark.sqlContext.read.format("csv").option("header", "true").option("delimiter", "|").option("ignoreLeadingWhiteSpace", "true").option("ignoreTrailingWhiteSpace", "true").load("/Users/kd/git/spark/log-scraper/src/main/resources/out/generated/Item/part-00000-f1fad064-9cbc-4b0b-88fa-7621559fca0c-c000.csv")`

`val location_tb = spark.sqlContext.read.format("csv").option("header", "true").option("delimiter", "|").option("ignoreLeadingWhiteSpace", "true").option("ignoreTrailingWhiteSpace", "true").load("/Users/kd/git/spark/log-scraper/src/main/resources/out/generated/Location/part-00000-cbb2d419-913a-4be1-9dcf-f8fc7ce04404-c000.csv")`

## create a temporary view to query later
create a temporary view to query later, so we can join or query to see the data

`item.createOrReplaceTempView("item")`
`location_tb.createOrReplaceTempView("location_tb")`

## Query data from the view
query from the view created earlier

`spark.sql("select Product,Address,Date from location_tb").show()`
`spark.sql("select Item_ID,Date from item").show()`

## Join two views
Join query will be used to fetch data from the both views

`spark.sql("SELECT itm.Item_ID,loc.Address FROM item as itm left join location_tb as loc on itm.Item_ID = loc.Product ").show()`

## show columns of the views
print the columns of the views

`location_tb.columns.foreach(println)`
`item.columns.foreach(println)`