package org.apache.jena.sparql.service.enhancer.concurrent;

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

//  private void checkLinked(LinkedListNode<T> node) {
//      if (!node.isLinked()) {
//          throw new IllegalArgumentException("Node is not linked.");
//      }
//  }
}
