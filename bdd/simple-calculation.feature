@logging
@no_capture
Feature: Index Factory calculation with a straightforward methodology

  Scenario: Performs an index calculation
    Given we have a local serverless instance running
     When we define a new index "US Equity" (us-equity) depending on markets US,Canada
     And we upload a CSV file with number of shares as of 2019-12-31 for market US
     And we upload a CSV file with daily prices as of 2020-01-31 for market US
     Then the us-equity components as of 2020-01-31 are
      | component | market value |
      | A         | 74670000     |
      | B         | 32050000     |
      | C         | 2320000      |
      | D         | 47130000     |
      | E         | 177890000    |
      | X         | 177890000    |
      | Y         | 177890000    |
      | Z         | 177890000    |
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
     When we define a new index "US Equity Div" (us-equity-div) depending on markets US
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
