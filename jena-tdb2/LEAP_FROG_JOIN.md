# Leap Frog Join Implementation for Apache Jena TDB2

## Overview

This implementation adds a "leap frog" join algorithm to TDB2 that efficiently joins multiple basic graph patterns by leveraging the sorted nature of B+Tree indices.

## Files

### Core Implementation

1. **LeapFrogJoinIteratorOptimized.java** - The optimized N-way leap frog join iterator
   - Coordinates multiple sorted iterators (not just 2-way)
   - Uses B+Tree sorted nature for efficient matching
   - Maintains current tuples from each iterator and advances them in a coordinated "leap frog" pattern
   - Finds minimum value across all iterators and advances others to catch up
   - Leverages the sorted order to skip non-matching ranges efficiently

2. **StageGeneratorLeapFrogJoin.java** - Stage generator for leap frog joins
   - Implements the `StageGenerator` interface
   - Detects patterns with shared variables
   - Falls back to standard execution for patterns without shared variables
   - Can be chained with other stage generators

3. **StageGeneratorDirectTDB.java** - Modified to handle leap frog joins
   - Integrated leap frog join detection
   - Falls back to standard PatternMatchTDB2 for single patterns

### Test Coverage

**TestLeapFrogJoinOptimized.java** - Test cases for leap frog join:
- leapFrog_01: Two-way join with shared variable
- leapFrog_02: Three-way join
- leapFrog_withVariables: Join with variables in both patterns
- leapFrog_singlePattern: Single pattern (should use standard execution)

## Algorithm

The leap frog join algorithm works as follows:

1. **Find join variables**: Identify variables shared across all patterns in the BGP

2. **Execute patterns in parallel**: Run each pattern as a separate query to get sorted iterators

3. **Leap frog coordination**:
   - Find the iterator with the minimum join key value
   - Seek all other iterators to at least that minimum value using B+Tree properties
   - When all join variables match across all iterators, produce a joined result
   - Advance iterators and repeat until one is exhausted

4. **Early termination**: When an iterator's value exceeds another's minimum, skip ahead efficiently

## Benefits

Compared to existing approaches:

- **Hash joins**: No materialization needed, better memory efficiency
- **Nested loop joins**: O(n+m) instead of O(n*m), leverages sorted indices
- **Chained binary joins**: Considers global join pattern

## Usage

The leap frog join is automatically activated when:
1. A BGP has 2+ triple patterns
2. Patterns share common variables
3. The dataset is backed by TDB2

To use explicitly with a custom context:

```java
Context cxt = ARQ.getContext().copy();
StageGenerator orig = StageBuilder.chooseStageGenerator(cxt);
StageGenerator leapFrog = new StageGeneratorLeapFrogJoin(orig);
cxt.set(ARQ.stageGenerator, leapFrog);

// Then execute queries with the custom context
```

## Integration Points

- Works with existing TDB2 architecture (BindingNodeId, StageMatchTuple, etc.)
- Uses TupleIndexRecord.find() for efficient B+Tree range queries
- Maintains compatibility with all existing TDB2 features

## Limitations

- Currently only supports patterns with common variables across all patterns
- Fall back to standard execution for patterns without shared variables
- Requires sorted indices (B+Tree) for optimal performance

## Optimization Strategy

The key optimization in the leap frog join is:

1. **Min-find**: Find the iterator with the smallest join key value
2. **Seek-ahead**: Advance other iterators to at least that minimum value
3. **Match detection**: When all iterators are at or past the minimum, check for matches
4. **Skip non-matches**: If no match, advance the minimum iterator and repeat

This approach leverages the sorted nature of B+Tree indices to skip non-matching ranges efficiently, avoiding the O(n*m) comparisons of nested loop joins.
