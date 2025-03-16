package org.apache.jena.sparql.service.enhancer.impl;

import java.util.Iterator;
import java.util.function.Predicate;

import org.apache.jena.sparql.service.enhancer.claimingcache.PredicateRangeSet;
import org.apache.jena.sparql.service.enhancer.claimingcache.PredicateTrue;
import org.apache.jena.sparql.service.enhancer.util.LinkedList;
import org.junit.Test;

import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;

public class TestLinkedList {
    @Test
    public void test() {
        RangeSet<Long> rs = TreeRangeSet.create();
        rs.add(Range.closedOpen(0l, 11l));


        LinkedList<Predicate<Long>> list = new LinkedList<>();
        list.append(PredicateTrue.get());
        list.append(PredicateTrue.get());
        list.append(new PredicateRangeSet<>(rs));

        System.out.println(list);

        Iterator<?> it = list.iterator();
        it.hasNext();
        it.next();
        // it.remove();

        it.next();
        it.remove();
        System.out.println(list);
    }
}
