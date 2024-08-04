# Exposed Plugin

This plugin enables Assertainty integration with [Exposed](https://jetbrains.github.io/Exposed/home.html).
It is parameterized in `org.jetbrains.exposed.sql.Table` and `org.jetbrains.exposed.sql.Expression`.

### Gradle

```Kotlin
testImplementation("io.github.peterattardo.assertainty:exposed-plugin:0.2.0")
```

## Usage

```Kotlin
val db = // create Database object
val table = // create Table object
    
db.transaction { // opens a transaction block in which to execute
    table.assert {
        +table.column1
        +table.column2
        
        always(table.column4 greater 0)
    }
}    
    
table.assert(db) { // automatically creates a transaction block from the db object in which to execute
    +table.column1
    +table.column2
    
    always(table.column4 greater 0)
}
```

> [!NOTE]
> Exposed requires that all database access happens within a `transaction` block. 
> This includes serialization of `Table` and `Expression` objects.
> You can either manually create the block, or if you pass the `Database` object to the `assert` function, one will be created internally.