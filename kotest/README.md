# Kotest Plugin

This plugin translates Assertainty `assert` blocks into Kotest tests.
It is built as an extension function on Kotest's `RootScope`

## Usage

```Kotlin
class DataTest : StringSpec({
    "someTest"(this) {
        someTable.assert {
            //assertion code
        } // returns an AssertionsBlockResult will be translated into individual tests
    }

    val columnSerializer = { column: Column ->
        // Default test names include the columns of the assertion. 
        // This optional function allows you to specify a more readable string representation of a column than its existing `toString()`
    }
    "someOtherTest"(this, columnSerializer) {
        someTable.assert {
            //assertion code
        }
    }
})
```

>[!WARNING]
> If using the Exposed plugin, any attempt to serialize a column outside of a `transaction` block will raise an exception.
> This includes when Assertainty creates tests that include the columns in the name.
> To address this, either wrap the test definitions in a transaction block, or implement a safe `columnSerializer` function.