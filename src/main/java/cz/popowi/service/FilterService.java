package cz.popowi.service;

import cz.popowi.model.TesterDeviceBugView;
import org.apache.spark.sql.Dataset;

import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class FilterService {
    private Dataset<TesterDeviceBugView> dataset;

    private final String ALL_ALIAS = "ALL";
    private final String SEPARATOR = ",";

    public FilterService() {
        dataset = new DatasetService().getJoinedDataset();
    }

    public void filter(String countryFilter, String deviceFilter) {
        Set<String> countryFilters = splitInput(countryFilter);
        Set<String> deviceFilters = splitInput(deviceFilter);
        if (ALL_ALIAS.equals(countryFilter) && ALL_ALIAS.equals(deviceFilter)) {
            showAll();
        } else if (ALL_ALIAS.equals(countryFilter)) {
            filterDevices(deviceFilters);
        } else if (ALL_ALIAS.equals(deviceFilter)) {
            filterCountries(countryFilters);
        } else {
            filterByCountryAndDevice(countryFilters, deviceFilters);
        }
    }

    public Set<String> splitInput(String input) {
        return Stream.of(input.split(SEPARATOR)).collect(Collectors.<String>toSet());
    }

    private void showAll() {
        showOutput(dataset.sort(dataset.col("count").desc()));
    }

    private void filterDevices(Set<String> deviceFilters) {
        showOutput(dataset.filter(dataset.col("description").isInCollection(deviceFilters)));
    }

    private void filterCountries(Set<String> countryFilters) {
        showOutput(dataset.filter(dataset.col("country").isInCollection(countryFilters)));
    }

    private void filterByCountryAndDevice(Set<String> countryFilters, Set<String> deviceFilters) {
        showOutput(dataset.filter(
                dataset.col("description").isInCollection(deviceFilters)
                        .and(dataset.col("country").isInCollection(countryFilters))));
    }

    private void showOutput(Dataset<TesterDeviceBugView> dataset) {
        dataset.sort(dataset.col("count").desc())
                .withColumnRenamed("firstName", "First Name")
                .withColumnRenamed("lastName", "Last Name")
                .withColumnRenamed("description", "Device")
                .withColumnRenamed("count", "Experience")
                .show();
    }

}
