package cz.popowi.reader

import cz.popowi.model.Device
import cz.popowi.service.DatasetService
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.types.Metadata
import org.apache.spark.sql.types.StructField
import spock.lang.Specification

class DeviceReaderTest extends Specification {

    static DeviceReader reader

    def setupSpec() {
        DatasetService datasetService = new DatasetService()
        Properties properties = new Properties()
        properties.setProperty("dataset.device", "src/main/resources/devices.csv")
        reader = new DeviceReader(datasetService.sparkSession, datasetService.options, properties)
    }

    def "should read all records from csv"() {
        when:
        Dataset<Device> dataset = reader.readAll()

        then:
        dataset.count() == 10
    }

    def "should return imported dataset with proper fields"() {
        given:
        StructField[] structFields = [
                new StructField("deviceId", DataTypes.StringType, true, Metadata.empty()),
                new StructField("description", DataTypes.StringType, true, Metadata.empty())
        ]

        when:
        Dataset<Device> dataset = reader.readAll()

        then:
        dataset.schema().fields() as Set == structFields as Set
    }
}
