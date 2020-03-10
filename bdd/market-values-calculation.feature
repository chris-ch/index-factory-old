@logging
@no_capture
Feature: Index Factory market values calculation

  Scenario: Performs an index calculation
    Given we have a local serverless instance running
     When we define a new index "Equities North America" (na-equity) depending on markets US,Canada
     And we upload a CSV file with number of shares as of 2019-12-31 for market US
     And we upload a CSV file with number of shares as of 2019-12-31 for market Canada
     And we upload a CSV file with daily prices as of 2020-01-31 for market US
     And we upload a CSV file with daily prices as of 2020-01-31 for market Canada
     Then we do nothing for 8 seconds
     And the na-equity components as of 2020-01-31 are
      | component | market value |
      | A         | 74670000     |
      | B         | 32050000     |
      | C         | 2320000      |
      | D         | 47130000     |
      | E         | 177890000    |
      | X         | 64670000     |
      | Y         | 42050000     |
      | Z         | 2120000      |
