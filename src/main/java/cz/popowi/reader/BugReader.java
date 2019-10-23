package cz.popowi.reader;

import cz.popowi.model.Bug;
import lombok.AllArgsConstructor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

import java.util.Map;
import java.util.Properties;

@AllArgsConstructor
public class BugReader {

    SparkSession sparkSession;
    Map<String, String> options;
    Properties properties;

    public Dataset<Bug> readAll() {
        return sparkSession
                .read()
                .options(options)
                .csv(properties.getProperty("dataset.bug"))
                .as(Encoders.bean(Bug.class))
                .cache();
    }
}
