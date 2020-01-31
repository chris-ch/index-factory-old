Feature: Index Factory calculation

  Scenario: Performs an index calculation with a straightforward methodology
    Given we have a local serverless instance running
     When we define a new index starting 2020-01-01
     When we upload a CSV file with number of shares as of 2019-12-31
     And we upload a CSV file with daily prices as of 2020-01-31
     Then the index value is updated
     And we upload a CSV file with daily prices as of 2020-02-03
     Then the index value is updated
     And we upload a CSV file with daily prices as of 2020-02-28
     Then the index value is updated
     And we upload a CSV file with daily prices as of 2020-03-02
     Then the index value is updated
     And we upload a CSV file with daily dividends as of 2020-03-04
     And we upload a CSV file with daily prices as of 2020-03-31
     Then the index value is updated
     When we upload a CSV file with number of shares as of 2020-03-31
     And we upload a CSV file with daily prices as of 2020-04-06
     Then the index value is updated
