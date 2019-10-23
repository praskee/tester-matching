package cz.popowi.service

import spock.lang.Specification

class FilterServiceTest extends Specification {

    private final FilterService filterService = new FilterService()



    def "should split input with comma separator"() {
        when:
        def splitedSet = filterService.splitInput("1,2,3,4,5")

        then:
        splitedSet.containsAll(["1", "2", "3", "4", "5"])
    }
}
