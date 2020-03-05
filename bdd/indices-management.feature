@logging
@no_capture
Feature: Indices management

  Scenario: Creating and retrieving indices
    Given we have a local serverless instance running
     When we define a new index "North American Equities" (na-equities) depending on markets US,Canada
     And we define a new index "US Equities" (us-equities) depending on markets US
     Then querying index us-equities returns index with name "US Equities"
     And querying indices for market US returns "us-equities,na-equities"
     And querying indices for market Canada returns "na-equities"
