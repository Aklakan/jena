# Iter Close-Behavior Analysis

Analysis of `org.apache.jena.atlas.iterator.Iter` — which methods close the underlying iterator.

Source: `jena-base/src/main/java/org/apache/jena/atlas/iterator/Iter.java`

---

## Methods that **close** the iterator upon return

| Method | Line | Mechanism |
|--------|------|-----------|
| `apply(Iterator, Consumer)` (static) | 241 | `try/finally { Iter.close(iter) }` |
| `collect(Iterator, Supplier, BiConsumer)` (static) | 232 | delegates to `apply` |
| `collect(Iterator, Collector)` (static) | 226 | delegates to `collect(iter, supplier, accumulator)` → `apply` |
| `toList(Iterator)` (static) | 137 | delegates to `collect` → `apply` |
| `toSet(Iterator)` (static) | 132 | delegates to `collect` → `apply` |
| `allMatch(Iterator, Predicate)` (static) | 334 | `try/finally { Iter.close(iter) }` |
| `anyMatch(Iterator, Predicate)` (static) | 352 | `try/finally { Iter.close(iter) }` |
| `noneMatch(Iterator, Predicate)` (static) | 371 | delegates to `anyMatch` |
| `take(Iterator, int)` (static) | 577 | fully consumes `IterLimit`, which calls `closeOnce()` |
| `iterator(Iterator)` (static) | 145 | `toList(iterator)` → `collect` → `apply` → close |
| `materialize(Iterator)` (static) | 830 | `toList(iter)` → close |
| `log(PrintStream, Iterator)` (static) | 757 | `Iter.toList(iter)` → close |
| `asString(Iterator, String)` (static) | 708 | `.collect(Collectors.joining(...))` → `apply` → close |
| `asString(Iterator, CharSequence, ...)` (static) | 713 | same as above |
| `close(Iterator)` (static) | 718 | explicit close |
| `Iter#close()` (instance) | 943 | `Iter.close(iterator)` |
| `Iter#toList()` (instance) | 963 | `toList(iterator)` → `collect` → `apply` → close |
| `Iter#toSet()` (instance) | 958 | `toSet(iterator)` → `collect` → `apply` → close |
| `Iter#collect(Supplier, BiConsumer)` (instance) | 1079 | `collect(iterator, ...)` → `apply` → close |
| `Iter#collect(Collector)` (instance) | 1084 | `collect(iterator, ...)` → `apply` → close |
| `Iter#allMatch(Predicate)` (instance) | 994 | static `allMatch` → close |
| `Iter#anyMatch(Predicate)` (instance) | 998 | static `anyMatch` → close |
| `Iter#noneMatch(Predicate)` (instance) | 1002 | `noneMatch` → `anyMatch` → close |
| `Iter#take(int)` (instance) | 1102 | static `take` fully consumes → close |

## Methods that do **NOT** close the iterator

| Method | Line | Note |
|--------|------|------|
| `forEach(Iterator, Consumer)` (static) | 68 | just `forEachRemaining` |
| `foldLeft(Iterator, R, Folder)` (static) | 161 | plain loop |
| `foldRight(Iterator, R, Folder)` (static) | 170 | recursive, no close |
| `reduce(Iterator, BinaryOperator)` (static) | 178 | delegates to reduce with identity |
| `reduce(Iterator, T, BinaryOperator)` (static) | 183 | plain loop |
| `min(Iterator, Comparator)` (static) | 193 | plain loop |
| `max(Iterator, Comparator)` (static) | 208 | plain loop |
| `findFirst(Iterator, Predicate)` (static) | 380 | plain loop, no close |
| `findLast(Iterator, Predicate)` (static) | 394 | plain loop, no close |
| `findAny(Iterator, Predicate)` (static) | 409 | delegates to `findFirst` |
| `step(Iterator, int)` (static) | 565 | advances, no close |
| `skip(Iterator, long)` (static) | 684 | advances, no close |
| `count(Iterator)` (static) | 696 | `forEachRemaining`, no close |
| `consume(Iterator)` (static) | 703 | `forEachRemaining`, no close |
| `print(PrintStream, Iterator)` (static) | 790 | `forEachRemaining`, no close |
| `sendToSink(Iterator, Sink)` (static) | 795 | closes the *sink*, not the iterator |
| `first(Iterator)` / `first(Iterator, Predicate)` (static) | 858/863 | plain loop, no close |
| `firstIndex(Iterator, Predicate)` (static) | 873 | plain loop, no close |
| `last(Iterator)` / `last(Iterator, Predicate)` (static) | 883/888 | plain loop, no close |
| `debug(PrintStream, Iterator)` (static) | 778 | returns `IterMap` (lazy close) |
| `asStream(Iterator)` / `asStream(Iterator, boolean)` (static) | 72/76 | close deferred to `Stream.onClose` |
| `Iter#forEach(Consumer)` (instance) | 948 | `forEachRemaining`, no close |
| `Iter#forEachRemaining(Consumer)` (instance) | 953 | `forEachRemaining`, no close |
| `Iter#sendToSink(Sink)` (instance) | 967 | closes sink, not iterator |
| `Iter#first()` / `first(Predicate)` (instance) | 971/980 | no close |
| `Iter#firstIndex(Predicate)` (instance) | 985 | no close |
| `Iter#last()` (instance) | 975 | no close |
| `Iter#findFirst(Predicate)` (instance) | 1006 | no close |
| `Iter#findAny(Predicate)` (instance) | 1010 | no close |
| `Iter#findLast(Predicate)` (instance) | 1014 | no close |
| `Iter#foldLeft(R, Folder)` (instance) | 1051 | no close |
| `Iter#foldRight(R, Folder)` (instance) | 1055 | no close |
| `Iter#reduce(...)` (instance) | 1062/1066 | no close |
| `Iter#min(Comparator)` (instance) | 1070 | no close |
| `Iter#max(Comparator)` (instance) | 1074 | no close |
| `Iter#apply(Consumer)` (instance) | 1089 | `forEachRemaining`, **no close** (inconsistent with static `apply`) |
| `Iter#count()` (instance) | 1156 | `forEachRemaining`, no close |
| `Iter#skip(long)` (instance) | 1151 | advances, no close |

## Methods that return a **lazily-closing** wrapper

These return iterators that will close the underlying iterator when the wrapper is exhausted or explicitly `.close()`'d:

| Method | Line | Wrapper |
|--------|------|---------|
| `filter(Iterator, Predicate)` (static) | 251 | `IterFiltered` – closes in `hasNext()`/`next()`/`close()` |
| `map(Iterator, Function)` (static) | 419 | `IterMap` – closes via `close()` |
| `operate(Iterator, Consumer)` (static) | 473 | `IterOperate` – closes via `close()` |
| `limit(Iterator, long)` (static) | 627 | `IterLimit` – closes in `hasNext()`/`close()` |
| `distinct(Iterator)` (static) | 533 | `IterFiltered` (via `filter`) |
| `distinctCached(Iterator, int)` (static) | 541 | `IterFiltered` (via `filter`) |
| `distinctAdjacent(Iterator)` (static) | 549 | `IterFiltered` (via `filter`) |
| `removeNulls(Iterator)` (static) | 554 | `IterFiltered` (via `filter`) |
| `takeWhile(Iterator, Predicate)` (static) | 589 | `IteratorTruncate` |
| `dropWhile(Iterator, Predicate)` (static) | 609 | `IteratorDropWhile` |
| `flatMap(Iterator, Function)` (static) | 465 | `IteratorFlatMap` |
| `Iter#filter(Predicate)` (instance) | 990 | `IterFiltered` |
| `Iter#map(Function)` (instance) | 1024 | `IterMap` |
| `Iter#operate(Consumer)` (instance) | 1047 | `IterOperate` |
| `Iter#limit(long)` (instance) | 1146 | `IterLimit` |
| `Iter#takeWhile/takeUntil/dropWhile/dropUntil` (instance) | 1111–1141 | `IteratorTruncate`/`IteratorDropWhile` |
| `Iter#distinct()/distinctAdjacent()` (instance) | 1166/1174 | `IterFiltered` |
| `Iter#removeNulls()` (instance) | 1019 | `IterFiltered` |

---

## Notable inconsistency

The static `apply(Iterator, Consumer)` (line 241) closes the iterator in a `finally` block, but the instance method `Iter#apply(Consumer)` (line 1089) does **not** close—it just calls `forEachRemaining`. This is likely a bug or oversight.
