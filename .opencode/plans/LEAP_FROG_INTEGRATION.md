# Integration Plan: Leap Frog Join Tests into TDB2 Test Suite

## Overview

This plan integrates the leap frog join implementation into the existing TDB2 test suite so that all existing tests can run against a dataset with leap frog join enabled. This provides comprehensive validation that the leap frog join doesn't break any existing functionality.

## Current Test Architecture

### Test Suite Organization

1. **TS_* Classes**: Test suites using JUnit Platform Suite API
   - Only classes matching `**/TS_*.java` and `**/Scripts_*.java` are run by default
   - Examples: `TS_Store.java`, `TS_SolverTDB.java`, `TS_TDB2Factory.java`

2. **Abstract Test Classes**: Base classes with test methods
   - Tests implement abstract methods (e.g., `createDataset()`)
   - Examples from ARQ:
     - `AbstractTestQueryExec` - Query execution tests
     - `AbstractTestJoin` - Join algorithm tests
     - `GraphsTests` - Graph manipulation tests
   - Examples from TDB2:
     - `AbstractTestGraphsTDB2` - Graph tests for TDB2
     - `AbstractTestStoreConnectionBasics` - Store connection tests

3. **Concrete Test Classes**: Implement abstract base classes
   - `TestQueryExecTDB` extends `AbstractTestQueryExec`
   - `TestGraphsTDB2_A` extends `AbstractTestGraphsTDB2`

## Integration Strategy

### Step 1: Create Leap Frog Dataset Factory

**File**: `src/test/java/org/apache/jena/tdb2/DatasetFactoryLeapFrog.java`

```java
public class DatasetFactoryLeapFrog {
    public static Dataset createDatasetLeapFrog() {
        Dataset ds = TDB2Factory.createDataset();
        StageGenerator orig = StageBuilder.chooseStageGenerator(ds.getContext());
        StageGenerator leapFrog = new StageGeneratorLeapFrogJoin(orig);
        ds.getContext().set(ARQ.stageGenerator, leapFrog);
        return ds;
    }
}
```

### Step 2: Create Leap Frog Test Classes

#### TestQueryExecLeapFrogTDB

**File**: `src/test/java/org/apache/jena/tdb2/store/TestQueryExecLeapFrogTDB.java`

Extends `AbstractTestQueryExec` to run all query execution tests with leap frog join.

```java
public class TestQueryExecLeapFrogTDB extends AbstractTestQueryExec {
    @Override
    protected Dataset createDataset() {
        return DatasetFactoryLeapFrog.createDatasetLeapFrog();
    }
    
    @Override
    protected void releaseDataset(Dataset ds) {
        ds.abort();
        TL.expel(ds);
    }
}
```

#### TestJoinLeapFrogTDB

**File**: `src/test/java/org/apache/jena/tdb2/store/TestJoinLeapFrogTDB.java`

Extends `AbstractTestJoin` to run join tests with leap frog join.

```java
public class TestJoinLeapFrogTDB extends AbstractTestJoin {
    private static Dataset dataset;
    
    @BeforeAll
    public static void beforeClass() {
        dataset = DatasetFactoryLeapFrog.createDatasetLeapFrog();
    }
    
    @Override
    protected void executeTest(...) {
        // Execute join with leap frog enabled
    }
    
    @AfterAll
    public static void afterClass() {
        dataset.abort();
        TL.expel(dataset);
    }
}
```

#### TestGraphsLeapFrogTDB

**File**: `src/test/java/org/apache/jena/tdb2/graph/TestGraphsLeapFrogTDB.java`

Extends `AbstractTestGraphsTDB2` to run graph tests with leap frog join.

```java
public class TestGraphsLeapFrogTDB extends AbstractTestGraphsTDB2 {
    @Override
    protected Dataset createDataset() {
        return DatasetFactoryLeapFrog.createDatasetLeapFrog();
    }
}
```

### Step 3: Add to Test Suites

#### Modify TS_Store.java

Add the new test classes to the store test suite:

```java
@Suite
@SelectClasses({
    // ... existing classes ...
    TestQueryExecLeapFrogTDB.class
    , TestJoinLeapFrogTDB.class
    , TestGraphsLeapFrogTDB.class
})
public class TS_Store {
    // ...
}
```

#### Modify TS_SolverTDB.java

Add leap frog specific solver tests:

```java
@Suite
@SelectClasses({
    TestSolverTDB.class
    , TestStats.class
    , TestLeapFrogSolverTDB.class  // New
})
public class TS_SolverTDB {
    // ...
}
```

## Expected Test Count Increase

### Current Tests: 863

### With Leap Frog Integration:

Assuming we add leap frog to the major test suites:

1. **AbstractTestQueryExec** (~15 tests)
   - TestQueryExecLeapFrogTDB: +15 tests
   
2. **AbstractTestJoin** (~25 tests)
   - TestJoinLeapFrogTDB: +25 tests
   
3. **GraphsTests** (~30 tests)
   - TestGraphsLeapFrogTDB: +30 tests
   
4. **AbstractTestQueryExecutionCancel** (~15 tests)
   - TestQueryExecutionCancel_TDB2_LeapFrog: +15 tests

**Total additional tests**: ~85 tests
**New total**: ~948 tests (863 + 85)

## Benefits

1. **Comprehensive Validation**: All existing tests run with leap frog join
2. **Regression Detection**: Any bugs in leap frog join will be caught by existing tests
3. **Performance Testing**: Can compare execution times between standard and leap frog
4. **Maintainability**: Minimal new test code needed
5. **Coverage**: Leverages existing test infrastructure

## Implementation Order

1. Create `DatasetFactoryLeapFrog.java` helper
2. Create `TestQueryExecLeapFrogTDB.java` (extends AbstractTestQueryExec)
3. Add to `TS_Store.java` and verify tests pass
4. Create `TestJoinLeapFrogTDB.java` (extends AbstractTestJoin)
5. Create `TestGraphsLeapFrogTDB.java` (extends AbstractTestGraphsTDB2)
6. Add to appropriate test suites
7. Run full test suite to verify all tests pass

## Considerations

1. **Test Performance**: Leap frog tests may be slower due to additional validation
2. **Test Isolation**: Each test class needs proper dataset lifecycle management
3. **Failing Tests**: If leap frog is buggy, tests will fail - this is expected
4. **Conditional Execution**: Consider making leap frog tests optional or in a separate profile

## Future Enhancements

1. **Performance Comparison**: Add tests that measure and compare performance
2. **Load Testing**: Test with larger datasets to verify leap frog optimization
3. **Parallel Execution**: Run standard and leap frog versions in parallel for comparison
4. **Coverage Reporting**: Track which existing tests are now covered by leap frog

## Notes

- The leap frog stage generator should be a wrapper around the existing TDB2 stage generator
- Tests should verify that results are identical between standard and leap frog execution
- The integration should not modify existing TDB2 code, only add test infrastructure
