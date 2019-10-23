package cz.popowi.reader;

import cz.popowi.model.Tester;
import lombok.AllArgsConstructor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

import java.util.Map;
import java.util.Properties;

@AllArgsConstructor
public class TesterReader {

    SparkSession sparkSession;
    Map<String, String> options;
    Properties properties;

    public Dataset<Tester> readAll() {
        return sparkSession.read()
                .options(options)
                .csv(properties.getProperty("dataset.tester"))
                .as(Encoders.bean(Tester.class))
                .cache();
    }
}
