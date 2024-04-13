package org.kd.log;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.spark.sql.functions.col;

public class Main {
    public static void main(String[] args) {
        System.out.println("Hello world!");

        Logger.getLogger("org.apache").setLevel(Level.WARN);


        SparkSession sparkSession = SparkSession.builder().appName("Gym Competitor").master("local[*]").getOrCreate();


//        Dataset<Row> inputData = sparkSession.read()
//                .option("delimiter", ", ")
//                .option("header", true)
//                .csv("./src/main/resources/inputfile.csv");
//        inputData.show(false);
//
//        Dataset<Row> inputData2 = sparkSession.read()
//                .option("delimiter", ",")
//                .option("header", true)
//                .csv("./src/main/resources/inputfile2.csv");
//        inputData2.show(false);


        Dataset<Row> logData = sparkSession
                .read()
                .option("header", false)
                .text("./src/main/resources/spark_log.log");

        Dataset<Row> filteredLogData = logData
                .filter(col("value")
                        .startsWith("|").or(col("value").contains("XYZ_DATASET")));


//                .forEach( e-> {
//
//
//            System.out.println(e.toString());
//        });


        // write to csv

        filteredLogData

                .coalesce(1)
                .write()
                .option("header", "false")
                .mode("overwrite")
                .csv("");


        AtomicReference<String> dtName = new AtomicReference<>("");

        List<String> filtered = filteredLogData.coalesce(1).collectAsList()
                .stream()
                .map(e -> {
                    String row = e.getString(0);
                    StringBuilder formattedRow = new StringBuilder();

                    String datasetName = "";
                    Pattern pattern = Pattern.compile("XYZ_DATASET: \\{(.*?)\\}");
                    Matcher matcher = pattern.matcher(row);

                    if (matcher.find()) {
                        dtName.set(matcher.group(1));
                        System.out.println(datasetName);
                        return "==================================";

                    } else {
                        return formattedRow.append(row)
                                .append(dtName.get())
                                .append("|").toString();

                    }
                })
                .collect(Collectors.toList());


        List<List<String>> groupedDs = new ArrayList<>();
        AtomicReference<List<String>> innerRow = new AtomicReference<>();

        IntStream.range(0, filtered.size())
                .forEach((e) -> {

                    String recrod = filtered.get(e);

                    if(recrod.contains("==================================")) {
                        if(innerRow.get() != null && innerRow.get().size() > 0) {
                            groupedDs.add(innerRow.get());
                        }
                       innerRow.set(new ArrayList<>());
                    } else {
                        innerRow.get().add(recrod);

                    } if(e == filtered.size()-1) {
                        groupedDs.add(innerRow.get());
                    }
                });



        List<Dataset<Row>> finalDstoWriteToFiles = new ArrayList<>();

        for(List entry : groupedDs) {

            Dataset<Row> dataset = sparkSession.createDataset(entry, Encoders.STRING()).toDF("value");
//            finalDstoWriteToFiles.add(dataset);
//                    .write()
//                    .format("csv")
//                    .option("header", "false") // Include header in the CSV file
//                    .mode("overwrite") // Overwrite the file if it already exists
//                    .save("./src/main/resources/output");
        }





        Dataset<Row> cleanedDataset = sparkSession.createDataset(filtered, Encoders.STRING()).toDF("value");







        System.out.println("######");


    }

    // Function to split dataset based on the line delimiter

    private static double[] splitDataset(Dataset<Row> data) {
        // Get the count of lines in the dataset
        long count = data.count();

        // Create an array to hold the split points
        double[] splits = new double[(int) count];

        // Initialize the split points
        int index = 0;
        for (Row row : data.collectAsList()) {
            String line = row.getString(0);
            if (line.contains("XYZ_DATASET") || index == 0) {
                // If the line contains "+", mark it as a split point
                splits[index] = 1.0;
                int nextIndex = index + 1;
                splits[nextIndex] = 1.0;
            } else {
                // Otherwise, mark it as not a split point
                splits[index] = 0.0;
            }
            index++;
        }

        int lastIndex = (int) count - 1;

        splits[lastIndex] = 1.0;

        return splits;
    }
}