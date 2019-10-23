package cz.popowi.reader;

import cz.popowi.model.Device;
import lombok.AllArgsConstructor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

import java.util.Map;
import java.util.Properties;

@AllArgsConstructor
public class DeviceReader {

    SparkSession sparkSession;
    Map<String, String> options;
    Properties properties;

    public Dataset<Device> readAll() {
        return sparkSession
                .read()
                .options(options)
                .csv(properties.getProperty("dataset.device"))
                .as(Encoders.bean(Device.class))
                .cache();
    }
}
