@logging
@no_capture
Feature: Index Factory calculation with a straightforward methodology

  Scenario: Performs an index calculation
    Given we have a local serverless instance running
     When we upload a CSV file with daily prices as of 2020-02-03 for market US
     And we upload a CSV file with daily prices as of 2020-02-28 for market US
     Then we have got 2 files for 2020-02 for market US
