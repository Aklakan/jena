/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.jena.sparql.service.enhancer.concurrent;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.ListIterator;

import org.junit.Assert;
import org.junit.Test;

public class TestIdPool {
    @Test
    public void test01() {
        IdPool pool = new IdPool();
        int id0 = pool.acquire();
        Assert.assertEquals(0, id0);

        int id1 = pool.acquire();
        Assert.assertEquals(1, id1);

        int id2 = pool.acquire();
        Assert.assertEquals(2, id2);

        // We have not given back any ids to the pool so no id can be recycled.
        Assert.assertEquals(0, pool.getRecyclePoolSize());

        // Give back and re-acquire ids starting from the lowest.
        // Giving back a single id and immediately re-acquiring a new one should
        // return the same id.

        pool.giveBack(id0);
        id0 = pool.acquire();
        Assert.assertEquals(id0, 0);

        pool.giveBack(id1);
        id1 = pool.acquire();
        Assert.assertEquals(id1, 1);

        pool.giveBack(id2);
        id2 = pool.acquire();
        Assert.assertEquals(id2, 2);

        // We have re-acquired all ids so there shouldn't be any recycled ones.
        Assert.assertEquals(0, pool.getRecyclePoolSize());

        // Give back and re-acquire ids starting from the highest.

        pool.giveBack(id2);
        id2 = pool.acquire();
        Assert.assertEquals(id2, 2);

        pool.giveBack(id1);
        id1 = pool.acquire();
        Assert.assertEquals(id1, 1);

        pool.giveBack(id0);
        id0 = pool.acquire();
        Assert.assertEquals(id0, 0);

        // Giving back all but the highest ids should track those ids in the recycle pool
        pool.giveBack(id0);
        pool.giveBack(id1);

        Assert.assertEquals(2, pool.getRecyclePoolSize());

        // Giving back the highest id should now clear the recycle pool
        pool.giveBack(id2);
        Assert.assertEquals(0, pool.getRecyclePoolSize());

        // Since all ids were given back then next acquired one should be 0 again
        id0 = pool.acquire();
        Assert.assertEquals(0, id0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void test_invalid_giveback_01() {
        IdPool pool = new IdPool();
        int id0 = pool.acquire();
        Assert.assertEquals(0, id0);

        pool.giveBack(id0);
        pool.giveBack(id0);
    }

    @Test
    public void test_random_01() {
        IdPool pool = new IdPool();
        List<Integer> ids = new ArrayList<>();

        for (int j = 0; j < 20; ++j) {
            // Acquire n=10 ids and make sure there are no duplicates
            for (int i = 0; i < 10; ++i) {
                int x = pool.acquire();
                if (ids.contains(x)) {
                    throw new RuntimeException("Error - got an id that is already in use");
                }
                ids.add(x);
            }

            // Shuffle the ids and give back the last m=8 ids
            Collections.shuffle(ids);
            ListIterator<Integer> it = ids.listIterator(ids.size());
            for (int i = 0; i < 8 && it.hasPrevious(); ++i) {
                int id = it.previous();
                it.remove();
                pool.giveBack(id);
            }
        }

        // Return all remaining ids
        ids.forEach(pool::giveBack);

        // Assert that the recycle pool is empty since all ids were given back
        Assert.assertEquals(0, pool.getRecyclePoolSize());

        // Assert that the next id we get is 0
        int id0 = pool.acquire();
        Assert.assertEquals(0, id0);
    }
}
