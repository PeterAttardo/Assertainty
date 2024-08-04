# Spark Plugin

This plugin enables Assertainty integration with [Apache Spark](https://spark.apache.org/), using the [Kotlin Spark API](https://github.com/Kotlin/kotlin-spark-api).
It is parameterized in `org.apache.spark.sql.Dataset` and `org.apache.spark.sql.Column`.

### Gradle

```Kotlin
testImplementation("io.github.peterattardo.assertainty:spark-plugin:0.2.0")
```

## Usage

```Kotlin
val ds = // create dataset
ds.assert {
    +Column("someColumn") // grouping column
    +"someOtherColumn" // the Spark DSL adds this convenience function to the core DSL to specify grouping columns by String.
    
    always(functions.length(Column("someIdColumn")) eq 15) // Because the plugin is parameterized in org.apache.spark.sql.Column, it can take full advantage of the methods available to that class. 
}

ds.assertSeparateQueries { // Logically identical to assert, but under the hood it runs each assertion as its own call to RelationalGroupedDataset#agg()
    +"someColumn"
    +"someOtherColumn"
    
    min_sum(Column("revenue"), 100_000) // we're making good money, eh?
    min_count(100) // averaging $1000/sale is impressive
}
```

> [!NOTE]
> Spark, like the other plugins, defaults to generating a single combined query.
> However, because of the likelihood of duplicate columns between assertions (count() in particular), all columns are aliased during the query building process.
> To avoid this behavior, `assertSeparateQueries` exists, in which each assertion gets its own aggregation call, at the cost of more iterations over the data and slower execution time.