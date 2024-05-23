# JUnit Plugin

This plugin translates KDataAssertions `assert` blocks into JUnit tests.
It is built around JUnit's `TestFactory` feature.

## Usage

```Kotlin
@TestFactory
fun someKDataAssertionTests() = dataAssertionTestFactory { // opens a block with a `TestFactoryScope` receiver
    columnSerializer { column ->
        // Default test names include the columns of the assertion. 
        // This optional function allows you to specify a more readable string representation of a column than its existing `toString()`
    }
    "someTest" { // creates a JUnit container node named "someTest", into which all the assertions will be created as individual tests
        someTable.assert {
            //assertion code
        } // returns an AssertionsBlockResult will be translated into individual tests
    }
    "someOtherTest" {
        someTable.assert {
            //assertion code
        }
    }
}
```

>[!WARNING]
> If using the Exposed plugin, any attempt to serialize a column outside of a `transaction` block will raise an exception.
> This includes when KDataAssertions creates tests that include the columns in the name.
> To address this, either wrap the test definitions in a transaction block, or implement a safe `columnSerializer` function.