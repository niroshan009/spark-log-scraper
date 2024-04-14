package org.kd.log;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.apache.spark.sql.functions.col;

public class Main {
    public static void main(String[] args) throws IOException {
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



        AtomicReference<String> dtName = new AtomicReference<>("");

        Pattern pattern = Pattern.compile("XYZ_DATASET: \\{(.*?)\\}");
        List<String> filtered = filteredLogData.coalesce(1).collectAsList()
                .stream()
                .map(e -> {
                    String row = e.getString(0);
                    StringBuilder formattedRow = new StringBuilder();

                    Matcher matcher = pattern.matcher(row);

                    if (matcher.find()) {
                        dtName.set(matcher.group(1));
                        System.out.println("******"+dtName.get());
                        return "XYZ_DATASET: {"+dtName.get()+"}";

                    } else {

                        row = row.replaceFirst("\\|","");
                        return formattedRow
                                .append("XYZ_DATA_ENTRY: {")
                                .append(dtName.get())
                                .append("}")
                                .append(row)
                                .append(dtName.get())
                                .toString();

                    }
                })
                .collect(Collectors.toList());


        Map<String,List<String>> groupedDs = new HashMap<>();

        AtomicReference<String> datasetName = new AtomicReference<>();


        IntStream.range(0, filtered.size())
                .forEach((e) -> {

                    String recrod = filtered.get(e);

                    Matcher matcher = pattern.matcher(recrod);

                    if(matcher.find()) {

                        if(!groupedDs.containsKey(matcher.group(1))) {
                            groupedDs.put(matcher.group(1),new ArrayList<>());
                        }
                        datasetName.set(matcher.group(1));
                    } else {

                        String[] dataRecordSplit = recrod.split("XYZ_DATA_ENTRY: \\{(.*?)\\}");

                        if(groupedDs.containsKey(datasetName.get())) {
                            groupedDs.get(datasetName.get()).add(dataRecordSplit[1]);
                        }

                    }
                });





        // writing the the files

        for(String datasetNmToSave : groupedDs.keySet()) {
            Dataset<Row> dataset = sparkSession.createDataset(groupedDs.get(datasetNmToSave), Encoders.STRING()).toDF("value");
            dataset.coalesce(1)
                    .write()
                    .mode("overwrite")
                    .csv("./src/main/resources/out/generated/"+datasetNmToSave);
        }





        Dataset<Row> cleanedDataset = sparkSession.createDataset(filtered, Encoders.STRING()).toDF("value");








        System.out.println("######");


    }







//            finalDstoWriteToFiles.add(dataset);
//                    .write()
//                    .format("csv")
//                    .option("header", "false") // Include header in the CSV file
//                    .mode("overwrite") // Overwrite the file if it already exists
//                    .save("./src/main/resources/output");

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