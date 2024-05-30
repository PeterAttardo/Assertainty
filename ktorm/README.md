# Ktorm Plugin

This plugin enables Assertainty integration with [Ktorm](https://www.ktorm.org/).
It is parameterized in `org.ktorm.dsl.QuerySource` and `org.ktorm.schema.ColumnDeclaring`.

### Gradle

```Kotlin
testImplementation("io.github.peterattardo.assertainty:ktorm-plugin:0.1.0")
```

## Usage

```Kotlin
val source = db.from(table)
source.assert {
    +table.column1
    +table.column2
    
    min_count_when(table.column3 eq 50, 10_000) // Because the plugin is parameterized in org.ktorm.schema.ColumnDeclaring, it can take full advantage of the methods available to that class. 
    always(table.column4 gt 0)
}

source.assert(where = table.column5 eq "2000-01-01") { // will apply the where filter after select is called on the `source` object
    +table.column1
    +table.column2

    min_count_when(table.column3 eq 50, 10_000)
    always(table.column4 gt 0)
}
```
>[!NOTE]
> Ktorm does not support subqueries at this time. 
> This means that Assertainty cannot accept an existing `Query`, and must be fed the root `QuerySource`.
> Any desired filtering on this `QuerySource` can be accomplished by passing in a value to the `where` parameter of the `assert` call.
