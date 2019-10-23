package cz.popowi;

import cz.popowi.model.Bug;
import cz.popowi.service.FilterService;
import org.apache.log4j.Logger;
import org.apache.log4j.Level;

import java.util.Scanner;

public class Main {

    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);

        FilterService filterService = new FilterService();

        Scanner scanner = new Scanner(System.in);
        String countryFilter;
        String deviceFilter;
        while(true) {
            System.out.println("\nSearch by criteria - press C+C to terminate");
            System.out.println("Country:");
            countryFilter = scanner.nextLine();
            System.out.println("Device:");
            deviceFilter = scanner.nextLine();
            filterService.filter(countryFilter, deviceFilter);
        }
    }
}
