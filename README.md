# Assertainty

_"Be certain about your data"_

Assertainty is a Kotlin library for writing and executing data quality test cases against tabular data of an arbitrary size. 
It is built on, and integrates with, popular data systems/connections like Spark and JDBC, as well as test harnesses like JUnit and Kotest. 
Because it relies on pre-existing data processing systems, it can scale to as large as the existing infrastructure. 
If you can query it, Assertainty can test it.

Assertainty provides a simple DSL, an example of which can be seen below:

```kotlin
@TestFactory
fun test() = dataAssertionTestFactory {
    "inspectData" {
        val table = // { code for selecting today's data }
        table.assert {
            min_count(10000) //we expect at least 10,000 new rows per day 
            unique(table.id) //we expect the ids to never collide
            always(table.email regexp_like "^[a-zA-Z0-9._%-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,4}$") //we expect emails to be valid
            max_ratio_when(table.status eq lit("failed"), 0.05) // we expect failure rate to not exceed 5% 
        }
    }
}
```

## How it works

Assertainty seeks to bridge the gap between the scale and power of distributed systems, and the convenience and tooling of local development.
It accomplishes this by splitting the workload; code locally and compute remotely.
Assertainty generates aggregation queries to submit to existing systems, and then processes assertions on the resulting aggregated metrics.

#### Benefits
This paradigm makes Assertainty exceptionally lightweight, in many ways.
* From an infrastructure perspective, it requires no, or next to no, new infrastructure.
It can either be run locally on a developer's machine, or on a single small node within a data pipeline. 
All the heavy computation is offloaded to your existing infrastructure.
* From a bandwidth perspective, not even a sample of the full data is transferred; only a small summary dataset of aggregated metrics is required.
* From a cognitive load perspective, new learnings are absolutely minimal.
It integrates with tools and frameworks developers are already familiar with.

#### Limitations
The primary limitation of Assertainty is that it operates on aggregations of your data rather than individual rows.
This generally means that test failures report in the form of how many rows failed the assertion, or what some total sum or count was, and not what specific rows led to failures.
However, well crafted assertions will give you very good coverage over your data, and a specific place to start your investigation.

## Plugins

Assertainty is built on a core-and-plugin model, where the core module is generic in types `Table` and `Column`, and plugin modules inherit from it to implement specific integrations.
There are data source plugins that enable connections to specific engines/connections, and testing plugins that integrate with test harnesses.

### Data Source Plugins

Currently, there are four plugins enabling connections to big data systems:
* [Spark](spark/) for working with [Apache Spark](https://spark.apache.org/) dataframes
* [Exposed](exposed/) for working with the [Jetbrains Exposed](https://jetbrains.github.io/Exposed/home.html) SQL ORM library
* [Ktorm](spark/) for working with the [Ktorm](https://www.ktorm.org/) SQL ORM library
* [Raw SQL](rawsql/) is a thin wrapper around raw SQL strings and a user-provided connection

### Test Plugins

* [JUnit](junit/) for writing tests in JUnit
* [Kotest](kotest/) for writing tests in Kotest

For specific details on the use of these plugins, please click the links above.

## Usage

As mentioned, `core` is parameterized in `Table` and `Column`. 
Each Data Source Plugin specifies concrete classes for those parameters, but the behavior between plugins is thereafter much the same.
Plugins are built around the `assert` extension function on their respective `Table` class:

```Kotlin
table.assert { // opens an assert block with a `TableScope` as a receiver.
    +someGroupingColumn // column on which to group the table. Assertions will be computed and run for each value within the group
    +someOtherGroupingColumn // same as above. Groups are the cartesian product of all grouping columns.
    +aThirdGroupingColumn // why not
    
    equal(someAggregationColumn, value) // assertion that checks if the aggregation is == the threshold
    equal(someAggregationColumn, someOtherAggregationColumn) // assertion that checks if the aggregation is == the other aggregation
    min(someAggregationColumn, thresholdValue) // assertion that checks if the aggregation is >= the threshold
    max(someAggregationColumn, thresholdValue) // assertion that checks if the aggregation is <= the threshold
    min_sum(someColumn, thresholdValue) // assertion that checks if the sum of the column is >= the threshold
    max_sum(someColumn, thresholdValue) // assertion that checks if the sum of the column is <= the threshold
    min_avg(someColumn, thresholdValue) // assertion that checks if the average of the column is >= the threshold
    max_avg(someColumn, thresholdValue) // assertion that checks if the average of the column is <= the threshold
    min_count(thresholdValue) // assertion that checks if the count of rows is >= the threshold
    max_count(thresholdValue) // assertion that checks if the count of rows is <= the threshold
    min_when(someConditionColumn, thresholdValue) // assertion that checks if the count where the condition is true is >= the threshold
    max_when(someConditionColumn, thresholdValue) // assertion that checks if the count where the condition is true is <= the threshold
    min_distinct(someColumn, thresholdValue) // assertion that checks if the count of distinct values of the column >= the threshold
    max_distinct(someColumn, thresholdValue) // assertion that checks if the count of distinct values of the column <= the threshold
    min_ratio_when(someConditionColumn, thresholdValue) // assertion that checks if the ratio between the count where the condition is true and the total count is >= the threshold
    max_ratio_when(someConditionColumn, thresholdValue) // assertion that checks if the ratio between the count where the condition is true and the total count is <= the threshold
    max_duplicates(someColumn, thresholdValue) // assertion that checks if the number of duplicates is <= the threshold
    max_duplicate_ratio(someColumn, thresholdValue) // assertion that checks if the ratio of the count of duplicates to the total count is <= the threshold
    max_null_ratio(someColumn, thresholdValue) // assertion that checks if the ratio of the count of nulls to the total count is <= the threshold
    never(someConditionColumn) // assertion that checks if the condition is never true
    always(someConditionColumn) // assertion that checks if the condition is always true
    never_null(someColumn) // assertion that checks if the column is never null
    unique(someColumn) // assertion that checks that the column has no duplicates
    
    assertion1<Double>(someAggregationColumn) {computedGroups: Map<Column, Any?>, computedMetric: Double ->
        //custom assertion of one metric column. This block should call at least one kotlin.test assertion function
    }
    assertion2<Int, Double>(someAggregationColumn, someOtherAggregationColumn) {computedGroups: Map<Column, Any?>, computedMetric1: Int, computedMetric2: Double ->
        //custom assertion of two metric columns. This block should call at least one kotlin.test assertion function
    }
    assertion(someAggregationColumn, someOtherAggregationColumn, aThirdAggregationColumn) {computed: Computed<Column> ->
        //custom assertion of [n] metric columns. This block should call at least one kotlin.test assertion function
    }
} // returns an AssertionBlockResults<Column>
```

This block will return an instance of `AssertionBlockResults<Column>` which is a typealias for `Map<DataAssertion<Column>, List<DataAssertionResult<Column>>>`. 
The Test Plugins know how to convert this to test cases for their respective test harness, and you can read the specifics for a given Test Plugin at the links above.

### Usage outside of test harnesses

Because the `assert` block returns an instance of `AssertionBlockResults`, it can be used directly, even outside of formal tests.
One use case would be to insert data validation checks into a pipeline, and then report failures or block downstream processing based on the results.
Assertainty provides a few convenience functions in this vein.

```Kotlin
table.assert {
    // desired assertions
}.let {
    if(it.anyFailed()) {
        // code to block downstream processing
    }
    it.forEachFailed { assertion, result ->
        // upload failures to reporting solution
    }
}
```