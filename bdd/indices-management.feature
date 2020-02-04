@logging
@no_capture
Feature: Indices management

  Scenario: Creating and retrieving indices
    Given we have a local serverless instance running
     When we define a new index "North American Equities" (na-equities) starting on 2020-01-01 depending on markets US,Canada
     And we define a new index "US Equities" (us-equities) starting on 2020-01-01 depending on markets US
     Then querying indices for market US returns "us-equities,na-equities"
     And querying indices for market Canada returns "na-equities"
