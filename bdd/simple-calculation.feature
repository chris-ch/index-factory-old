@logging
@no_capture
Feature: Index Factory calculation with a straightforward methodology

  Scenario: Performs an index calculation
    Given we have a local serverless instance running
     When we define a new index US Equity (us-equity) starting on 2020-01-01 depending on markets US,Canada
     And we upload a CSV file with number of shares as of 2019-12-31 for market US
     And we upload a CSV file with daily prices as of 2020-01-31 for market US
     Then the us-equity index value is 100.0
     When we upload a CSV file with daily prices as of 2020-02-03 for market US
     Then the us-equity index value is 100.0
     When we upload a CSV file with daily prices as of 2020-02-28 for market US
     Then the us-equity index value is 100.0
     When we upload a CSV file with daily prices as of 2020-03-02 for market US
     Then the us-equity index value is 100.0
     When we upload a CSV file with daily prices as of 2020-03-31 for market US
     Then the us-equity index value is 100.0
     When we upload a CSV file with number of shares as of 2020-03-31 for market US
     When we upload a CSV file with daily prices as of 2020-04-06 for market US
     Then the us-equity index value is 100.0

  Scenario: Performs an index calculation including dividends
    Given we have a local serverless instance running
     When we define a new index US Equity Div (us-equity-div) starting on 2020-01-01 depending on markets US
     And we upload a CSV file with number of shares as of 2019-12-31 for market US
     And we upload a CSV file with daily prices as of 2020-01-31 for market US
     Then the us-equity-div index value is 100.0
     When we upload a CSV file with daily prices as of 2020-02-03 for market US
     Then the us-equity-div index value is 100.0
     When we upload a CSV file with daily prices as of 2020-02-28 for market US
     Then the us-equity-div index value is 100.0
     When we upload a CSV file with daily prices as of 2020-03-02 for market US
     Then the us-equity-div index value is 100.0
     When we upload a CSV file with dividends as of 2020-03-04 for market US
     When we upload a CSV file with daily prices as of 2020-03-31 for market US
     Then the us-equity-div index value is 100.0
     When we upload a CSV file with number of shares as of 2020-03-31 for market US
     When we upload a CSV file with daily prices as of 2020-04-06 for market US
     Then the us-equity-div index value is 100.0
