@logging
@no_capture
Feature: Index Factory calculation with a straightforward methodology

  Scenario: Performs an index calculation
    Given we have a local serverless instance running
     When we upload a CSV file with number of shares as of 2019-12-31 for market US
     Then the market US has number of shares dates:
      | as_of_date |
      | 2019-12-31 |

