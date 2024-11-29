package org.apache.jena.sparql.service.enhancer.concurrent;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.TreeSet;

public class IdPool {
    private volatile int i = 0;
    private final TreeSet<Integer> ids = new TreeSet<>();

    /** Return a free id. */
    public synchronized Integer acquire() {
        int result;
        if (!ids.isEmpty()) {
            Iterator<Integer> it = ids.iterator();
            result = it.next();
            it.remove();
        } else {
            result = i++;
        }
        return result;
    }

    /**
     * Return an id to the pool of available ids.
     *
     * @implNote If the highest acquired is returned then all consecutive
     *           trailing ids up to (excluding) the returned id are removed from the pool.
     *           In the worst case the will be overhead for completely emptying the pool.
     */
    public synchronized void giveBack(int v) {
        if (v >= i) {
            throw new IllegalArgumentException("Attempt to give back a value " + v + " which greater than the largest generated one " + i);
        }

        if (v + 1 == i) {
            // Case where the highest acquired value is returned
            --i;
            Iterator<Integer> it = ids.descendingIterator();
            while (it.hasNext()) {
                int id = it.next();
                if (id + 1 == i) {
                    it.remove();
                    --i;
                } else {
                    break;
                }
            }
        } else {
            if (ids.contains(v)) {
                throw new IllegalArgumentException("The value has already been given back: " + v);
            }

            ids.add(v);
        }
    }

    public static void main(String[] args) {
        IdPool pool = new IdPool();
        List<Integer> ids = new LinkedList<>();

        for (int j = 0; j < 100; ++j) {
            for (int i = 0; i < 10; ++i) {
                int x = pool.acquire();
                if (ids.contains(x)) {
                    throw new RuntimeException("Error - got id that is already in use");
                }
                ids.add(x);
            }

            Iterator<Integer> it = ids.iterator();
            for (int i = 0; i < 5 && it.hasNext(); ++i) {
                int id = it.next();
                it.remove();
                pool.giveBack(id);
            }
        }

        ids.forEach(pool::giveBack);

        int got = pool.acquire();
        System.out.println(got);
    }
}
