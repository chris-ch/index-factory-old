@logging
@no_capture
Feature: Index Factory market values calculation

  Scenario: Performs an index calculation
    Given we have a local serverless instance running
     When we define a new index US Equity (us-equity) depending on markets US,Canada
     And we upload a CSV file with number of shares as of 2019-12-31 for market US
     And we upload a CSV file with daily prices as of 2020-01-31 for market US
     Then the us-equity components as of 2020-01-31 are
      | component | market value |
      | A         | 74670000     |
      | B         | 32050000     |
      | C         | 2320000      |
      | D         | 47130000     |
      | E         | 177890000    |
