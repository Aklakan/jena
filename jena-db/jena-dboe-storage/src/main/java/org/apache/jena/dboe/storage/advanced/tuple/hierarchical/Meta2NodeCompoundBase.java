package org.apache.jena.dboe.storage.advanced.tuple.hierarchical;

import org.apache.jena.dboe.storage.advanced.tuple.TupleAccessor;

public abstract class Meta2NodeCompoundBase<D, C, V>
    extends Meta2NodeBase<D, C, V>
    implements Meta2NodeCompound<D, C, V>
{

    public Meta2NodeCompoundBase(int[] tupleIdxs, TupleAccessor<D, C> tupleAccessor) {
        super(tupleIdxs, tupleAccessor);
    }

//    public Meta2NodeCompoundBase(Meta2Node<D, C, V> child) {
//        super();
//        this.child = child;
//    }
//
//    @Override
//    public Meta2Node<D, C, V> getChild() {
//        return child;
//    }
}