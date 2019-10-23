package cz.popowi.reader

import cz.popowi.model.Tester
import cz.popowi.service.DatasetService
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.types.Metadata
import org.apache.spark.sql.types.StructField
import spock.lang.Specification

class TesterReaderTest extends Specification {

    static TesterReader reader

    def setupSpec() {
        DatasetService datasetService = new DatasetService()
        Properties properties = new Properties()
        properties.setProperty("dataset.tester", "src/main/resources/testers.csv")
        reader = new TesterReader(datasetService.sparkSession, datasetService.options, properties)
    }

    def "should read all records from csv"() {
        when:
        Dataset<Tester> dataset = reader.readAll()

        then:
        dataset.count() == 9
    }

    def "should return imported dataset with proper fields"() {
        given:
        StructField[] structFields = [
                new StructField("testerId", DataTypes.StringType, true, Metadata.empty()),
                new StructField("firstName", DataTypes.StringType, true, Metadata.empty()),
                new StructField("lastName", DataTypes.StringType, true, Metadata.empty()),
                new StructField("country", DataTypes.StringType, true, Metadata.empty()),
                new StructField("lastLogin", DataTypes.StringType, true, Metadata.empty())
        ]

        when:
        Dataset<Tester> dataset = reader.readAll()

        then:
        dataset.schema().fields() as Set == structFields as Set
    }
}
