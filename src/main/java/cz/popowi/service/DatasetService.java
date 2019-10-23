package cz.popowi.service;

import cz.popowi.model.BugDeviceView;
import cz.popowi.model.TesterDeviceBugView;
import cz.popowi.reader.BugReader;
import cz.popowi.reader.DeviceReader;
import cz.popowi.reader.TesterReader;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class DatasetService {

    public SparkSession sparkSession;
    public Properties properties;
    public  Map<String, String> options;

    private TesterReader testerReader;
    private DeviceReader deviceReader;
    private BugReader bugReader;

    public DatasetService() {
        initialize();
    }

    private void initialize() {
        loadProperties();

        sparkSession = SparkSession
                .builder()
                .appName("Java Spark SQL basic example")
                .config("spark.master", "local")
                .getOrCreate();

        options = new HashMap<>();
        options.put("header", "true");

        testerReader = new TesterReader(sparkSession, options, properties);
        deviceReader = new DeviceReader(sparkSession, options, properties);
        bugReader = new BugReader(sparkSession, options, properties);
    }

    private void loadProperties() {
        properties = new Properties();
        try {
            properties.load(new FileInputStream("src/main/resources/config.properties"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public Dataset<TesterDeviceBugView> getJoinedDataset() {

        Dataset<BugDeviceView> bugDeviceView = bugReader.readAll()
                .join(deviceReader.readAll(), "deviceId")
                .as(Encoders.bean(BugDeviceView.class));

        Dataset<TesterDeviceBugView> testerDeviceBugView = testerReader.readAll()
                .join(bugDeviceView, "testerId")
                .as(Encoders.bean(TesterDeviceBugView.class));

        return testerDeviceBugView
                .groupBy(
                        testerDeviceBugView.col("firstName"),
                        testerDeviceBugView.col("lastName"),
                        testerDeviceBugView.col("country"),
                        testerDeviceBugView.col("description")
                )
                .count()
                .as(Encoders.bean(TesterDeviceBugView.class))
                .cache();
    }
}
