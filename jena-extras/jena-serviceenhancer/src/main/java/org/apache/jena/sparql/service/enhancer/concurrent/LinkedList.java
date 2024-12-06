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

/**
 * A doubly linked list to keep track of idle executors in the ExecutorServicePool.
 * Each executor keeps a reference to a single node of this list.
 *
 * If the executor becomes busy then it unlinks itself from the list.
 * If the executor becomes idle then it appends itself to the end of this list with its idle timestamp.
 *
 * Consequently, the executors that have been idle longest are at the beginning of the list.
 * The cleanup task only has to release the idle executors at the beginning of the list.
 * The cleanup task can stop when encountering an executor whose idle time is too recent.
 *
 * This is not a fully fledged linked list implementation, e.g. it does not implement Collection.
 */
public class LinkedList<T> {
    public static class LinkedListNode<T> {
        private volatile T value;
        private volatile LinkedListNode<T> prev;
        private volatile LinkedListNode<T> next;
        private final LinkedList<T> list;

        private LinkedListNode(LinkedList<T> list) {
            super();
            this.list = list;
        }
        public T getValue() {
            return value;
        }
        public void setValue(T value) {
            this.value = value;
        }
        public LinkedListNode<T> getPrev() {
            return prev;
        }
        public LinkedListNode<T> getNext() {
            return next;
        }
        public LinkedList<T> getList() {
            return list;
        }
        public boolean isLinked() {
            // A node is linked if either (a) prev or next are non-null - or (b) the node is referenced by first.
            // A node is unlinked if prev and next are both null, and this node is not referenced by list.first.
            return prev != null || next != null || list.first == this;
        }
        public void unlink() {
            list.unlink(this);
        }
        public void moveToEnd() {
            list.moveToEnd(this);
        }
    }

    private volatile LinkedListNode<T> first;
    private volatile LinkedListNode<T> last;
    private volatile int size;

    public LinkedListNode<T> getFirst() {
        return first;
    }
    public LinkedListNode<T> getLast() {
        return last;
    }

    /** Create a new unlinked node. The node can only be inserted into this list. */
    public LinkedListNode<T> newNode() {
        return new LinkedListNode<>(this);
    }

    public LinkedListNode<T> append(T value) {
        LinkedListNode<T> result = newNode();
        result.value = value;
        moveToEnd(result);
        return result;
    }

    public void moveToEnd(LinkedListNode<T> node) {
        unlink(node);
        if (first == null) {
            first = node;
        } else {
            last.next = node;
            node.prev = last;
        }
        last = node;
        ++size;
    }

    /** Add node after the insert point. If the insert point is null then the node becomes first. */
    public void addAfter(LinkedListNode<T> insertPoint, LinkedListNode<T> node) {
        checkOwner(insertPoint);
        if (insertPoint == null) {
            if (first == null) {
                // Insert as first
                checkOwner(node); // node cannot be linked - if it was then first would not be null
                first = node;
                last = node;
            } else {
                // Insert before first
                unlink(node);
                LinkedListNode<T> tmp = first;
                tmp.prev = node;
                node.next = tmp;
                first = node;
            }
        } else {
            if (insertPoint.next != node) {
                unlink(node);

                LinkedListNode<T> tmp = insertPoint.next;
                insertPoint.next = node;
                node.prev = insertPoint;

                if (tmp != null) {
                    tmp.prev = node;
                    node.next = tmp;
                }

                if (last == insertPoint) {
                    last = node;
                }
            }
        }
    }

    private void checkOwner(LinkedListNode<T> node) {
        if (node.list != this) {
            throw new IllegalArgumentException("Cannot unlink a node that does not belong to this list.");
        }
    }

    private void unlink(LinkedListNode<T> node) {
        checkOwner(node);
        if (node.isLinked()) {
            if (node == first) {
                first = node.next;
            }
            if (node == last) {
                last = node.prev;
            }
            if (node.prev != null) {
                node.prev.next = node.next;
            }
            if (node.next != null) {
                node.next.prev = node.prev;
            }
            node.prev = null;
            node.next = null;
            --size;
        }
    }

    public int size() {
        return size;
    }
}
