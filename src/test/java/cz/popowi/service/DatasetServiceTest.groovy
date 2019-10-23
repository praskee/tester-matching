package cz.popowi.service

import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.types.Metadata
import org.apache.spark.sql.types.StructField
import spock.lang.Specification

class DatasetServiceTest extends Specification {

    private final DatasetService datasetService = new DatasetService()

    def "should return dataset with proper schema fields after join"() {
        given:
        StructField[] structFields = [
                new StructField("firstName", DataTypes.StringType, true, Metadata.empty()),
                new StructField("lastName", DataTypes.StringType, true, Metadata.empty()),
                new StructField("country", DataTypes.StringType, true, Metadata.empty()),
                new StructField("description", DataTypes.StringType, true, Metadata.empty()),
                new StructField("count", DataTypes.LongType, false, Metadata.empty()),
        ]

        when:
        def dataset = datasetService.getJoinedDataset()

        then:
        dataset.schema().fields() as Set == structFields as Set

    }
}
