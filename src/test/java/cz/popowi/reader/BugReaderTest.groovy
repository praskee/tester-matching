package cz.popowi.reader

import cz.popowi.model.Bug
import cz.popowi.service.DatasetService
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.types.Metadata
import org.apache.spark.sql.types.StructField
import spock.lang.Specification

class BugReaderTest extends Specification {

    static BugReader reader

    def setupSpec() {
        DatasetService datasetService = new DatasetService()
        Properties properties = new Properties()
        properties.setProperty("dataset.bug", "src/main/resources/bugs.csv")
        reader = new BugReader(datasetService.sparkSession, datasetService.options, properties)
    }

    def "should read all records from csv"() {
        when:
        Dataset<Bug> dataset = reader.readAll()

        then:
        dataset.count() == 1000
    }

    def "should return imported dataset with proper fields"() {
        given:
        StructField[] structFields = [
                new StructField("bugId", DataTypes.StringType, true, Metadata.empty()),
                new StructField("deviceId", DataTypes.StringType, true, Metadata.empty()),
                new StructField("testerId", DataTypes.StringType, true, Metadata.empty())
        ]

        when:
        Dataset<Bug> dataset = reader.readAll()

        then:
        dataset.schema().fields() as Set == structFields as Set
    }
}
