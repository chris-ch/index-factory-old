@logging
@no_capture
Feature: Index Factory calculation with a straightforward methodology

  Scenario: Performs an index calculation
    Given we have a local serverless instance running
     When we define a new index "US Equity" (us-equity) starting on 2020-01-01
     And we upload a CSV file with number of shares as of 2019-12-31
     And we upload a CSV file with daily prices as of 2020-01-31
     Then the index value is updated
     When we upload a CSV file with daily prices as of 2020-02-03
     Then the index value is updated
     When we upload a CSV file with daily prices as of 2020-02-28
     Then the index value is updated
     When we upload a CSV file with daily prices as of 2020-03-02
     Then the index value is updated
     When we upload a CSV file with daily prices as of 2020-03-31
     Then the index value is updated
     When we upload a CSV file with number of shares as of 2020-03-31
     When we upload a CSV file with daily prices as of 2020-04-06
     Then the index value is updated

  Scenario: Performs an index calculation inclduing dividends
    Given we have a local serverless instance running
     When we define a new index "US Equity Div" (us-equity-div) starting on 2020-01-01
     And we upload a CSV file with number of shares as of 2019-12-31
     And we upload a CSV file with daily prices as of 2020-01-31
     Then the index value is updated
     When we upload a CSV file with daily prices as of 2020-02-03
     Then the index value is updated
     When we upload a CSV file with daily prices as of 2020-02-28
     Then the index value is updated
     When we upload a CSV file with daily prices as of 2020-03-02
     Then the index value is updated
     When we upload a CSV file with dividends as of 2020-03-04
     When we upload a CSV file with daily prices as of 2020-03-31
     Then the index value is updated
     When we upload a CSV file with number of shares as of 2020-03-31
     When we upload a CSV file with daily prices as of 2020-04-06
     Then the index value is updated
