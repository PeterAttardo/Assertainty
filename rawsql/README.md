# Raw SQL Plugin

You drink your coffee black, you do the crossword puzzle in ink, and you don't see any reason to complicate the purity of SQL.
ORMs are for people who need to be reminded of the names and types of things. 
The Raw SQL Plugin is parameterized in `String` and `String`; it's strings all the way down.

### Gradle

```Kotlin
testImplementation("io.github.peterattardo.assertainty:rawsql-plugin:0.1.0")
```

## Usage

```Kotlin
val wrapper = JDBCSqlWrapper("some:connection:string")
"SELECT * FROM table".assert(wrapper) {
    +"someColumn"
    +"someOtherColumn"
    
    min_sum("aThirdColumn", 1_000_000)
    always("email REGEXP '^([a-zA-Z0-9_\\-\\.]+)@([a-zA-Z0-9_\\-\\.]+)\\.([a-zA-Z]{2,5})$'")
}

val dialect = // code for generating a custom dialect
"table".assert(wrapper, dialect) {
    //assertion code
}
```

### SqlWrapper

The Raw SQL Plugin's `assert` call takes an instance of `SqlWrapper` as a necessary parameter. 
This is a single-function interface that takes in a SQL select statement, and returns a `List<List<Any>>`, representing the result of the query.
A default implementation, `JDBCSqlWrapper`, is provided, but the interface can be used as a shim to implement any existing database connection code you may have.

### Dialect

The Raw SQL Plugin uses a default dialect object that generates standard SQL. 
However, if you are finding that the SQL generated is incompatible with your database, you can provide an instance of `SqlDialect` that allows you to control how the queries are constructed.