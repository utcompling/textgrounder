///////////////////////////////////////////////////////////////////////////////
//  collectiontuil.scala
//
//  Copyright (C) 2011-2013 Ben Wing, The University of Texas at Austin
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
///////////////////////////////////////////////////////////////////////////////

///////////////////////////////////////////////////////////////////////////////
//  Copyright (C) 2011-2013 Ben Wing, The University of Texas at Austin
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
///////////////////////////////////////////////////////////////////////////////

package opennlp.textgrounder
package util

import scala.util.control.Breaks._
import scala.collection.mutable
import scala.collection.immutable
import mutable.{Builder, MapBuilder}
import scala.collection.generic.CanBuildFrom
import scala.collection.{Map => BaseMap, GenTraversableLike, GenTraversable}
import scala.reflect.ClassTag

/**
 * A package containing various collection-related classes and functions.
 *
 * Provided:
 *
 * -- Default hash tables (which automatically provide a default value if
 *    no key exists; these already exist in Scala but this package makes
 *    them easier to use)
 * -- Dynamic arrays (similar to ArrayBuilder but specialized to use
 *    primitive arrays underlyingly, and allow direct access to that array)
 * -- LRU (least-recently-used) caches
 * -- Hash tables by range (which keep track of a subtable for each range
 *    of numeric keys)
 * -- A couple of miscellaneous functions:
 *    -- 'fromto', which does a range that is insensitive to order of its
 *       arguments
 *    -- 'merge_numbered_sequences_uniquely'
 */

protected class CollectionPackage {

  ////////////////////////////////////////////////////////////////////////////
  //                               Default maps                             //
  ////////////////////////////////////////////////////////////////////////////
  
  abstract class DefaultHashMap[T,U] extends mutable.HashMap[T,U] {
    def getNoSet(key: T): U
  }

  /**
   * Create a default hash table, i.e. a hash table where accesses to
   * undefined values automatically return 'defaultval'.  This class also
   * automatically sets the undefined key to 'defaultval' upon first
   * access to that key.  If you don't want this behavior, call getNoSet()
   * or use the non-setting variant below.  See the discussion below in
   * defaultmap() for a discussion of when setting vs. non-setting is useful
   * (in a nutshell, use the setting variant when type T is a mutable
   * collection; otherwise, use the nonsetting variant).
   */
  class SettingDefaultHashMap[T,U](
    create_default: T => U
  ) extends DefaultHashMap[T,U] {
    var internal_setkey = true

    override def default(key: T) = {
      val buf = create_default(key)
      if (internal_setkey)
        this(key) = buf
      buf
    }
        
    /**
     * Retrieve the value of 'key'.  If the value isn't found, the
     * default value (from 'defaultval') will be returned, but the
     * key will *NOT* added to the table with that value.
     *
     * FIXME: This code should have the equivalent of
     * synchronized(internal_setkey) around it so that it will work
     * in a multi-threaded environment.
     */
    def getNoSet(key: T) = {
      val oi_setkey = internal_setkey
      try {
        internal_setkey = false
        this(key)
      } finally { internal_setkey = oi_setkey }
    }
  }

  /**
   * Non-setting variant class for creating a default hash table.
   * See class SettingDefaultHashMap and function defaultmap().
   */
  class NonSettingDefaultHashMap[T,U](
    create_default: T => U
  ) extends DefaultHashMap[T,U] {
    override def default(key: T) = {
      val buf = create_default(key)
      buf
    }
        
    def getNoSet(key: T) = this(key)
  }

  /**
   * Create a default hash map that maps keys of type T to values of type
   * U, automatically returning 'defaultval' rather than throwing an exception
   * if the key is undefined.
   *
   * @param defaultval The default value to return.  Note the delayed
   *   evaluation using =>.  This is done on purpose so that, for example,
   *   if we use mutable Buffers or Sets as the value type, things will
   *   work: We want a *different* empty vector or set each time we call
   *   default(), so that different keys get different empty vectors.
   *   Otherwise, adding an element to the buffer associated with one key
   *   will also add it to the buffers for other keys, which is not what
   *   we want. (Note, when mutable objects are used as values, you need to
   *   set the `setkey` parameter to true.)
   * 
   * @param setkey indicates whether we set the key to the default upon
   *   access.  This is necessary when the value is something mutable, but
   *   probably a bad idea otherwise, since looking up a nonexistent value
   *   in the table will cause a later contains() call to return true on the
   *   value.
   * 
   * For example:
    
    val foo = defaultmap[String,Int](0, setkey = false)
    foo("bar")              -> 0
    foo contains "bar"      -> false
   
    val foo = defaultmap[String,Int](0, setkey = true)
    foo("bar")              -> 0
    foo contains "bar"      -> true         (Probably not what we want)
                   
   
    val foo = defaultmap[String,mutable.Buffer[String]](mutable.Buffer(), setkey = false)
    foo("myfoods") += "spam"
    foo("myfoods") += "eggs"
    foo("myfoods") += "milk"
    foo("myfoods")             -> ArrayBuffer(milk)                (OOOOOPS)
   
    val foo = defaultmap[String,mutable.Buffer[String]](mutable.Buffer(), setkey = true)
    foo("myfoods") += "spam"
    foo("myfoods") += "eggs"
    foo("myfoods") += "milk"
    foo("myfoods")             -> ArrayBuffer(spam, eggs, milk)    (Good)
   */
  def defaultmap[T,U](defaultval: => U, setkey: Boolean = false) = {
    def create_default(key: T) = defaultval
    if (setkey) new SettingDefaultHashMap[T,U](create_default _)
    else new NonSettingDefaultHashMap[T,U](create_default _)
  }
  /**
   * A default map where the values are mutable collections; need to have
   * `setkey` true for the underlying call to `defaultmap`.
   *
   * @see #defaultmap[T,U]
   */
  def collection_defaultmap[T,U](defaultval: => U) =
    defaultmap[T,U](defaultval, setkey = true)

  /**
   * A default map where the values are primitive types, and the default
   * value is the "zero" value for the primitive (0, 0L, 0.0 or false).
   * Note that this will "work", at least syntactically, for non-primitive
   * types, but will set the default value to null, which is probably not
   * what is wanted. (FIXME: Sort of.  It works but if you try to print
   * a result -- or more generally, pass to a function that accepts Any --
   * the boxed version shows up and you get null instead of 0.)
   *
   * @see #defaultmap[T,U]
   */
  def primmap[T,@specialized U](default: U = null.asInstanceOf[U]) = {
    defaultmap[T,U](default)
  }

  /**
   * A default map from type T to an Int, with 0 as the default value.
   *
   * @see #defaultmap[T,U]
   */
  def intmap[T](default: Int = 0) = defaultmap[T,Int](default)
  //def intmap[T]() = primmap[T,Int]()
  def longmap[T](default: Long = 0L) = defaultmap[T,Long](default)
  def shortmap[T](default: Short = 0) = defaultmap[T,Short](default)
  def bytemap[T](default: Byte = 0) = defaultmap[T,Byte](default)
  def doublemap[T](default: Double = 0.0d) = defaultmap[T,Double](default)
  def floatmap[T](default: Float = 0.0f) = defaultmap[T,Float](default)
  def booleanmap[T](default: Boolean = false) = defaultmap[T,Boolean](default)

  /**
   * A default map from type T to a string, with an empty string as the
   * default value.
   *
   * @see #defaultmap[T,U]
   */
  def stringmap[T](default: String = "") = defaultmap[T,String](default)

  /**
   * A default map which maps from T to an (extendable) array of type U.
   * The default value is an empty Buffer of type U.  Calls of the sort
   * `map(key) += item` will add the item to the Buffer stored as the
   * value of the key rather than changing the value itself. (After doing
   * this, the result of 'map(key)' will be the same collection, but the
   * contents of the collection will be modified.  On the other hand, in
   * the case of the above maps, the result of 'map(key)' will be
   * different.)
   * 
   * @see #setmap[T,U]
   * @see #mapmap[T,U,V]
   */
  def bufmap[T,U]() =
    collection_defaultmap[T,mutable.Buffer[U]](mutable.Buffer[U]())
  /**
   * A default map which maps from T to a set of type U.  The default
   * value is an empty Set of type U.  Calls of the sort
   * `map(key) += item` will add the item to the Set stored as the
   * value of the key rather than changing the value itself, similar
   * to how `bufmap` works.
   * 
   * @see #bufmap[T,U]
   * @see #mapmap[T,U,V]
   */
  def setmap[T,U]() =
    collection_defaultmap[T,mutable.Set[U]](mutable.Set[U]())
  /**
   * A default map which maps from T to a map from U to V.  The default
   * value is an empty Map of type U->V.  Calls of the sort
   * `map(key)(key2) = value2` will add the mapping `key2 -> value2`
   * to the Map stored as the value of the key rather than changing
   * the value itself, similar to how `bufmap` works.
   *   
   * @see #bufmap[T,U]
   * @see #setmap[T,U]
   */
  def mapmap[T,U,V]() =
    collection_defaultmap[T,mutable.Map[U,V]](mutable.Map[U,V]())
  /**
   * A default map which maps from T to a `primmap` from U to V, i.e.
   * another default map.  The default value is a `primmap` of type
   * U->V.  Calls of the sort `map(key)(key2) += value2` will add to
   * the value of the mapping `key2 -> value2` stored for `key` in the
   * main map.  If `key` hasn't been seen before, a new `primmap` is
   * created, and if `key2` hasn't been seen before in the `primmap`
   * associated with `key`, it will be initialized to the zero value
   * for type V.
   *
   * FIXME: Warning, doesn't always do what you want, whereas e.g.
   * `intmapmap` always will.
   *
   * @see #mapmap[T,U,V]
   */
   def primmapmap[T,U,V](default: V = null.asInstanceOf[V]) =
    collection_defaultmap[T,mutable.Map[U,V]](primmap[U,V](default))
  
  def intmapmap[T,U](default: Int = 0) =
    collection_defaultmap[T,mutable.Map[U,Int]](intmap[U](default))
  def longmapmap[T,U](default: Long = 0L) =
    collection_defaultmap[T,mutable.Map[U,Long]](longmap[U](default))
  def doublemapmap[T,U](default: Double = 0.0d) =
    collection_defaultmap[T,mutable.Map[U,Double]](doublemap[U](default))
  def stringmapmap[T,U](default: String = "") =
    collection_defaultmap[T,mutable.Map[U,String]](stringmap[U](default))

  // Another way to do this, using subclassing.
  //
  // abstract class defaultmap[From,To] extends HashMap[From, To] {
  //   val defaultval: To
  //   override def default(key: From) = defaultval
  // }
  //
  // class intmap[T] extends defaultmap[T, Int] { val defaultval = 0 }
  //

  // The original way
  //
  // def booleanmap[String]() = {
  //   new HashMap[String, Boolean] {
  //     override def default(key: String) = false
  //   }
  // }

  /////////////////////////////////////////////////////////////////////////////
  //                              Dynamic arrays                             //
  /////////////////////////////////////////////////////////////////////////////
  
  /**
   A simple class like ArrayBuilder but which gets you direct access
   to the underlying array and lets you easily reset things, so that you
   can reuse a Dynamic Array multiple times without constantly creating
   new objects.  Also has specialization.
   */
  class DynamicArray[@specialized T: ClassTag](initial_alloc: Int = 8) {
    protected val multiply_factor = 1.5
    var array = new Array[T](initial_alloc)
    var length = 0
    def ensure_at_least(size: Int) {
      if (array.length < size) {
        var newsize = array.length
        while (newsize < size)
          newsize = (newsize * multiply_factor).toInt
        val newarray = new Array[T](newsize)
        System.arraycopy(array, 0, newarray, 0, length)
        array = newarray
      }
    }

    def += (item: T) {
      ensure_at_least(length + 1)
      array(length) = item
      length += 1
    }

    def clear() {
      length = 0
    }
  }
    
  /////////////////////////////////////////////////////////////////////////////
  //                                Sorted lists                             //
  /////////////////////////////////////////////////////////////////////////////
  
  // Return a tuple (keys, values) of lists of items corresponding to a hash
  // table.  Stored in sorted order according to the keys.  Use
  // lookup_sorted_list(key) to find the corresponding value.  The purpose of
  // doing this, rather than just directly using a hash table, is to save
  // memory.

//  def make_sorted_list(table):
//    items = sorted(table.items(), key=lambda x:x[0])
//    keys = [""]*len(items)
//    values = [""]*len(items)
//    for i in xrange(len(items)):
//      item = items[i]
//      keys[i] = item[0]
//      values[i] = item[1]
//    return (keys, values)
//  
//  // Given a sorted list in the tuple form (KEYS, VALUES), look up the item KEY.
//  // If found, return the corresponding value; else return None.
//  
//  def lookup_sorted_list(sorted_list, key, default=None):
//    (keys, values) = sorted_list
//    i = bisect.bisect_left(keys, key)
//    if i != len(keys) and keys[i] == key:
//      return values[i]
//    return default
//  
//  // A class that provides a dictionary-compatible interface to a sorted list
//  
//  class SortedList(object, UserDict.DictMixin):
//    def __init__(self, table):
//      self.sorted_list = make_sorted_list(table)
//  
//    def __len__(self):
//      return len(self.sorted_list[0])
//  
//    def __getitem__(self, key):
//      retval = lookup_sorted_list(self.sorted_list, key)
//      if retval is None:
//        raise KeyError(key)
//      return retval
//  
//    def __contains__(self, key):
//      return lookup_sorted_list(self.sorted_list, key) is not None
//  
//    def __iter__(self):
//      (keys, values) = self.sorted_list
//      for x in keys:
//        yield x
//  
//    def keys(self):
//      return self.sorted_list[0]
//  
//    def itervalues(self):
//      (keys, values) = self.sorted_list
//      for x in values:
//        yield x
//  
//    def iteritems(self):
//      (keys, values) = self.sorted_list
//      for (key, value) in izip(keys, values):
//        yield (key, value)
//

//  //////////////////////////////////////////////////////////////////////////////
//  //                               Priority Queues                            //
//  //////////////////////////////////////////////////////////////////////////////
//
//  // Priority queue implementation, based on Python heapq documentation.
//  // Note that in Python 2.6 and on, there is a priority queue implementation
//  // in the Queue module.
//  class PriorityQueue(object):
//    INVALID = 0                     // mark an entry as deleted
//  
//    def __init__(self):
//      self.pq = []                         // the priority queue list
//      self.counter = itertools.count(1)    // unique sequence count
//      self.task_finder = {}                // mapping of tasks to entries
//  
//    def add_task(self, priority, task, count=None):
//      if count is None:
//        count = self.counter.next()
//      entry = [priority, count, task]
//      self.task_finder[task] = entry
//      heappush(self.pq, entry)
//  
//    //Return the top-priority task. If 'return_priority' is false, just
//    //return the task itself; otherwise, return a tuple (task, priority).
//    def get_top_priority(self, return_priority=false):
//      while true:
//        priority, count, task = heappop(self.pq)
//        if count is not PriorityQueue.INVALID:
//          del self.task_finder[task]
//          if return_priority:
//            return (task, priority)
//          else:
//            return task
//  
//    def delete_task(self, task):
//      entry = self.task_finder[task]
//      entry[1] = PriorityQueue.INVALID
//  
//    def reprioritize(self, priority, task):
//      entry = self.task_finder[task]
//      self.add_task(priority, task, entry[1])
//      entry[1] = PriorityQueue.INVALID
//
  ///////////////////////////////////////////////////////////////////////////
  //                    Least-recently-used (LRU) Caches                   //
  ///////////////////////////////////////////////////////////////////////////

  class LRUCache[T,U](maxsize: Int=1000) extends mutable.Map[T,U]
    with mutable.MapLike[T,U,LRUCache[T,U]] {
    val cache = mutable.LinkedHashMap[T,U]()

    // def length = return cache.length

    private def reprioritize(key: T) {
      val value = cache(key)
      cache -= key
      cache(key) = value
    }

    def get(key: T): Option[U] = {
      if (cache contains key) {
        reprioritize(key)
        Some(cache(key))
      }
      else None
    }
 
    override def update(key: T, value: U) {
      if (cache contains key)
        reprioritize(key)
      else {
        while (cache.size >= maxsize) {
          val (key2, value) = cache.head
          cache -= key2
        }
        cache(key) = value
      }
    }

    override def remove(key: T): Option[U] = cache.remove(key)
 
    def iterator: Iterator[(T, U)] = cache.iterator

    // All the rest Looks like pure boilerplate!  Why necessary?
    def += (kv: (T, U)): this.type = {
      update(kv._1, kv._2); this }
    def -= (key: T): this.type = { remove(key); this }

    override def empty = new LRUCache[T,U]()
    }

  // This whole object looks like boilerplate!  Why necessary?
  object LRUCache extends {
    def empty[T,U] = new LRUCache[T,U]()

    def apply[T,U](kvs: (T,U)*): LRUCache[T,U] = {
      val m: LRUCache[T,U] = empty
      for (kv <- kvs) m += kv
       m
    }

    def newBuilder[T,U]: Builder[(T,U), LRUCache[T,U]] =
      new MapBuilder[T, U, LRUCache[T,U]](empty)

    implicit def canBuildFrom[T,U]
      : CanBuildFrom[LRUCache[T,U], (T,U), LRUCache[T,U]] =
        new CanBuildFrom[LRUCache[T,U], (T,U), LRUCache[T,U]] {
          def apply(from: LRUCache[T,U]) = newBuilder[T,U]
          def apply() = newBuilder[T,U]
        }
  }

  ////////////////////////////////////////////////////////////////////////////
  //                           Hash tables by range                         //
  ////////////////////////////////////////////////////////////////////////////

  // A table that groups all keys in a specific range together.  Instead of
  // directly storing the values for a group of keys, we store an object (termed a
  // "collector") that the user can use to keep track of the keys and values.
  // This way, the user can choose to use a list of values, a set of values, a
  // table of keys and values, etc.
  
  // 'ranges' is a sorted list of numbers, indicating the
  // boundaries of the ranges.  One range includes all keys that are
  // numerically below the first number, one range includes all keys that are
  // at or above the last number, and there is a range going from each number
  // up to, but not including, the next number.  'collector' is used to create
  // the collectors used to keep track of keys and values within each range;
  // it is either a type or a no-argument factory function.  We only create
  // ranges and collectors as needed. 'lowest_bound' is the value of the
  // lower bound of the lowest range; default is 0.  This is used only
  // it iter_ranges() when returning the lower bound of the lowest range,
  // and can be an item of any type, e.g. the number 0, the string "-infinity",
  // etc.
  abstract class TableByRange[Coll,Numtype <% Ordered[Numtype]](
    ranges: Seq[Numtype],
    create: (Numtype)=>Coll
  ) {
    val min_value: Numtype
    val max_value: Numtype
    val items_by_range = mutable.Map[Numtype,Coll]()
    var seen_negative = false
  
    def get_collector(key: Numtype) = {
      // This somewhat scary-looking cast produces 0 for Int and 0.0 for
      // Double.  If you write it as 0.asInstanceOf[Numtype], you get a
      // class-cast error when < is called if Numtype is Double because the
      // result of the cast ends up being a java.lang.Integer which can't
      // be cast to java.lang.Double. (FMH!!!)
      if (key < null.asInstanceOf[Numtype])
        seen_negative = true
      var lower_range = min_value
      // upper_range = "infinity"
      breakable {
        for (i <- ranges) {
          if (i <= key)
            lower_range = i
          else {
            // upper_range = i
            break
          }
        }
      }
      if (!(items_by_range contains lower_range))
        items_by_range(lower_range) = create(lower_range)
      items_by_range(lower_range)
    }
  
    /**
     Return an iterator over ranges in the table.  Each returned value is
     a tuple (LOWER, UPPER, COLLECTOR), giving the lower and upper bounds
     (inclusive and exclusive, respectively), and the collector item for this
     range.  The lower bound of the lowest range comes from the value of
     'lowest_bound' specified during creation, and the upper bound of the range
     that is higher than any numbers specified during creation in the 'ranges'
     list will be the string "infinity" if such a range is returned.
  
     The optional arguments 'unseen_between' and 'unseen_all' control the
     behavior of this iterator with respect to ranges that have never been seen
     (i.e. no keys in this range have been passed to 'get_collector').  If
     'unseen_all' is true, all such ranges will be returned; else if
     'unseen_between' is true, only ranges between the lowest and highest
     actually-seen ranges will be returned.
     */
    def iter_ranges(unseen_between: Boolean=true, unseen_all: Boolean=false) = {
      var highest_seen: Numtype = 0.asInstanceOf[Numtype]
      val iteration_range =
        (List(if (seen_negative) min_value else 0.asInstanceOf[Numtype]) ++
          ranges) zip
         (ranges ++ List(max_value))
      for ((lower, upper) <- iteration_range) {
        if (items_by_range contains lower)
          highest_seen = upper
      }
  
      var seen_any = false
      for {(lower, upper) <- iteration_range
           // FIXME SCALABUG: This is a bug in Scala if I have to do this
           collector = items_by_range.getOrElse(lower, null.asInstanceOf[Coll])
           if (collector != null || unseen_all ||
               (unseen_between && seen_any &&
               upper != max_value && upper <= highest_seen))
           col2 = if (collector != null) collector else create(lower)
          }
      yield {
        if (collector != null) seen_any = true
        (lower, upper, col2)
      }
    }
  }

  class IntTableByRange[Coll](
    ranges: Seq[Int],
    create: (Int)=>Coll
  ) extends TableByRange[Coll,Int](ranges, create) {
    val min_value = java.lang.Integer.MIN_VALUE
    val max_value = java.lang.Integer.MAX_VALUE
  }

  class DoubleTableByRange[Coll](
    ranges: Seq[Double],
    create: (Double)=>Coll
  ) extends TableByRange[Coll,Double](ranges, create) {
    val min_value = java.lang.Double.NEGATIVE_INFINITY
    val max_value = java.lang.Double.POSITIVE_INFINITY
  }

 
//  //////////////////////////////////////////////////////////////////////////////
//  //                          Depth-, breadth-first search                    //
//  //////////////////////////////////////////////////////////////////////////////
//
//  // General depth-first search.  'node' is the node to search, the top of a
//  // tree.  'matches' indicates whether a given node matches.  'children'
//  // returns a list of child nodes.
//  def depth_first_search(node, matches, children):
//    nodelist = [node]
//    while len(nodelist) > 0:
//      node = nodelist.pop()
//      if matches(node):
//        yield node
//      nodelist.extend(reversed(children(node)))
//  
//  // General breadth-first search.  'node' is the node to search, the top of a
//  // tree.  'matches' indicates whether a given node matches.  'children'
//  // returns a list of child nodes.
//  def breadth_first_search(node, matches, children):
//    nodelist = deque([node])
//    while len(nodelist) > 0:
//      node = nodelist.popLeft()
//      if matches(node):
//        yield node
//      nodelist.extend(children(node))
//

 /////////////////////////////////////////////////////////////////////////////
 //                          General-purpose iterators                      //
 /////////////////////////////////////////////////////////////////////////////

  /**
   * An interruptible iterator that wraps an arbitrary iterator.  You can stop
   * further processing by calling `stop()`, which will make the iterator act
   * as if it is empty regardless of how many items are left in the wrapped
   * iterator. Note that calling `stop()` while mapping will stop iteration
   * *after* the result of the current mapping call is returned.
   */
  class InterruptibleIterator[T](iter: Iterator[T]) extends Iterator[T] {
    private var interrupted = false

    def hasNext = {
      if (interrupted) false
      else iter.hasNext
    }

    def next = {
      if (interrupted) 
        throw new java.util.NoSuchElementException("next on empty iterator")
      else iter.next
    }

    def stop() {
      interrupted = true
    }
  }

  /**
   * An empty iterator that executes some code when its contents are fetched.
   * This is useful for ensuring that side-effecting code gets executed at the
   * correct time in a complex iterator.
   *
   * @param body An expression or block of code to be executed when the iterator
   *   is tripped.
   */
  class SideEffectIterator(body: => Any) extends Iterator[Nothing] {
    var executed = false

    def hasNext = {
      if (!executed)
        body
      executed = true
      false
    }

    def next = {
      throw new java.util.NoSuchElementException("next on empty iterator")
    }
  }

  /**
   * An iterator that transposes a list of iterators by fetching "across"
   * rather than "down" the iterators.  That is, it fetches the first item
   * in each iterator and returns a list of those items, then fetches the
   * next item of each iterator, etc.  If some iterators are longer than
   * others, then the returned list at that point will only included values
   * from iterators for which such values still exist.
   */
  class TransposeIterator[T](iterators: Iterable[Iterator[T]]
    ) extends Iterator[Iterable[T]] {
    def hasNext = iterators.exists(_.hasNext)
    def next = iterators.collect {
      case iter@_ if iter.hasNext => iter.next
    }
  }

  /**
   * An iterator that wraps an arbitrary iterator, printing out each time
   * `hasNext` and `next` are called and the results of calling them.
   * This makes it easier to figure out the pattern by which a given iterator
   * is accessed.
   */
  class PrintIterator[T](iter: Iterator[T]) extends Iterator[T] {
    def hasNext = {
      val res = iter.hasNext
      println("hasNext: %s" format res)
      res
    }
    def next = {
      val res = iter.next
      println("next: %s" format res)
      res
    }
  }

  /**
   * An iterator to do the equivalent of a `groupBy` operation on a given
   * iterator, grouping by the key specified by `keyfun`.  Returns a series
   * of tuples of `(key, iterator)` where `key` is a key returned by `keyfun`
   * and `iterator` iterates over all consecutive items which return that
   * key when the keyfun is applied to them.
   */
  class GroupByIterator[T, K](
      iter: Iterator[T], keyfun: (T) => K
    ) extends Iterator[(K, Iterator[T])] {
    protected var curiter = iter.buffered
    def hasNext = curiter.hasNext
    def next = {
      val key = keyfun(curiter.head)
      val (leading, trailing) = curiter.span(keyfun(_) == key)
      curiter = trailing.buffered
      (key, leading)
    }
  }

  def duplicate_iterator[T,U](items: Iterable[T], iter: Iterator[U]):
      Iterable[(T, Iterator[U])] = {
    items.size match {
      case 0 => Iterable()
      case 1 => Iterable((items.head, iter))
      case _ => {
        val (head, tail) = (items.head, items.tail)
        val (left, right) = iter.duplicate
        Iterable((head, left)) ++ duplicate_iterator(tail, right)
      }
    }
  }

 /////////////////////////////////////////////////////////////////////////////
 //             Intersect/Union/Diff by key and specified combiner          //
 /////////////////////////////////////////////////////////////////////////////

  implicit class IntersectUnionWithPimp[K, A, Repr](a: GenTraversableLike[(K, A), Repr]) {
    private def occItems[B](sq: GenTraversable[(K, B)]) = {
      val occ = new mutable.HashMap[K, mutable.Buffer[B]] {
        override def default(key: K) = {
          val newbuf = mutable.Buffer[B]()
          this(key) = newbuf
          newbuf
        }
      }
      sq.foreach { case (k, v) =>
        occ(k) += v
      }
      occ
    }

    /**
     * Intersect two collections by their keys. This is identical to
     * `intersectWith` except that the combiner function is passed the
     * key as well as the two items to combine.
     *
     * @param b Other collection to intersect with.
     * @param combine Function to combine values from the two collections.
     */
    def intersectWithKey[B, R, That](b: GenTraversable[(K, B)])(
        combine: (K, A, B) => R)(
        implicit bf: CanBuildFrom[Repr, (K, R), That]): That = {
      (a, b) match {
        // FIXME! Should use implicit functions to handle this. But
        // then need to be careful to define special implicit versions of
        // functions like intersectWith() and intersectBy() that call this.
        // These need to be written specifically for IntMap, LongMap,
        // and HashMap. Either we need to duplicate all these functions
        // (YUCK!) or figure out some other way. Possibly we would need to
        // have a manifest passed in. But this should be a sort of manifest
        // that is allowed to not exist so we don't run into problems when
        // one isn't available.
        case (x: immutable.IntMap[A], y: immutable.IntMap[B]) =>
          x.intersectionWith[B, R](y, (k, p, q) =>
            combine(k.asInstanceOf[K], p.asInstanceOf[A], q)).
            asInstanceOf[That]
        case (x: immutable.LongMap[A], y: immutable.LongMap[B]) =>
          x.intersectionWith[B, R](y, (k, p, q) =>
            combine(k.asInstanceOf[K], p.asInstanceOf[A], q)).
            asInstanceOf[That]
        case _ => {
          val buf = bf()
          val occ = b.toMap
          if (occ.size == b.size) {
            // The easy way: no repeated items in `b`.
            a.foreach { case (key, value) =>
              if (occ contains key)
                buf += key -> combine(key, value, occ(key))
            }
          } else {
            // The hard way. Create a map listing all occurrences of each
            // key in `b`. Every time we find a key in `a`, iterate over
            // all occurrences in `b`.
            val occb = occItems(b)
            a.foreach { case (key, value) =>
              occb(key).foreach { value2 =>
                buf += key -> combine(key, value, value2)
              }
            }
          }
          buf.result
        }
      }
    }

    /**
     * Intersect two collections by their keys. Keep the ordering of
     * objects in the first collection. Use a combiner function to
     * combine items in common. If either item is a multi-map, then
     * for a key seen `n` times in the first collection and `m`
     * times in the second collection, it will occur `n * m` times
     * in the resulting collection, including all the possible
     * combinations of pairs of identical keys in the two collections.
     *
     * @param b Other collection to intersect with.
     * @param combine Function to combine values from the two collections.
     */
    def intersectWith[B, R, That](b: GenTraversable[(K, B)])(
        combine: (A, B) => R)(
        implicit bf: CanBuildFrom[Repr, (K, R), That]): That =
      a.intersectWithKey(b){ case (_, x, y) => combine(x, y) }(bf)

    /**
     * Union two collections by their keys. This is identical to
     * `unionWith` except that the combiner function is passed the
     * key as well as the two items to combine.
     *
     * @param b Other collection to union with.
     * @param combine Function to combine values from the two collections.
     */
    def unionWithKey[B >: A, That](b: GenTraversable[(K, B)])(
        combine: (K, A, B) => B)(
        implicit bf: CanBuildFrom[Repr, (K, B), That]): That = {
      // One way to write:
      // a.intersectWith(b)(combine) ++ a.diffWith(b) ++ b.diffWith(a)
      // Instead, we expand out the code to avoid extra work.
      // FIXME! Should use implicit functions to handle this. See
      // comment above in intersectWithKey().
      (a, b) match {
        case (x: immutable.HashMap[K, A], y: immutable.HashMap[K, B]) =>
          x.merged(y) { (p, q) =>
            (p._1, combine(p._1, p._2.asInstanceOf[A], q._2)) }.
            asInstanceOf[That]
        case (x: immutable.IntMap[A], y: immutable.IntMap[B]) =>
          x.unionWith[B](y, (k, p, q) =>
            combine(k.asInstanceOf[K], p.asInstanceOf[A], q)).
            asInstanceOf[That]
        case (x: immutable.LongMap[A], y: immutable.LongMap[B]) =>
          x.unionWith[B](y, (k, p, q) =>
            combine(k.asInstanceOf[K], p.asInstanceOf[A], q)).
            asInstanceOf[That]
        case _ => {
          val buf = bf()
          val occ = b.toMap
          if (occ.size == b.size) {
            // The easy way: no repeated items in `b`.
            a.foreach { case (key, value) =>
              if (occ contains key)
                buf += key -> combine(key, value, occ(key))
              else
                buf += key -> value
            }
          } else {
            val occb = occItems(b)
            a.foreach { case (key, value) =>
              // The hard way. See above.
              val values_b = occb(key)
              if (values_b.size > 0) {
                values_b.foreach { value2 =>
                  buf += key -> combine(key, value, value2)
                }
              } else
                buf += key -> value
            }
          }
          // Finally handle items in `b` but not `a`.
          val occa = new mutable.HashSet[K]
          a.foreach { case (key, value) =>
            occa += key
          }
          b.foreach { case (key, value) =>
            if (!(occa contains key))
              buf += key -> value
          }
          buf.result
        }
      }
    }

    /**
     * Union two collections by their keys. Keep the ordering of
     * objects in the first collection, followed by the second
     * collection. Use a combiner function to combine items in common.
     * If either item is a multi-map, then for a key seen `n` times
     * in the first collection and `m` times in the second collection,
     * it will occur `n * m` times in the resulting collection, including
     * all the possible combinations of pairs of identical keys in the
     * two collections.
     *
     * FIXME! We run into an error trying to call unionWith() on an
     * IntMap or LongMap because there's an existing unionWith(). Even
     * though that one takes two parameters, Scala isn't smart enough
     * to handle polymorphism between an implicit and non-implicit
     * function of the same name. This would be fixed if these functions
     * are defined directly in IntMap rather than as implicits.
     *
     * @param b Other collection to union with.
     * @param combine Function to combine values from the two collections.
     */
    def unionWith[B >: A, That](b: Traversable[(K, B)])(
        combine: (A, B) => B)(
        implicit bf: CanBuildFrom[Repr, (K, B), That]): That =
      a.unionWithKey(b){ case (_, x, y) => combine(x, y) }(bf)

    /**
     * Difference two collections by their keys. Keep the ordering of
     * objects in the first collection.
     *
     * FIXME: This doesn't quite work the way that plain `diff` works.
     * In our case, if a key occurs in `b`, all elements with the same
     * key in `a` are removed. In plain `diff`, only as many elements
     * are removed from `a` as occur in `b`. This makes sense with
     * multisets for simple objects but it's less clear whether this
     * makes sense with keys, because the values will typically be
     * different.
     *
     * @param b Other collection to difference with.
     */
    def diffWith[B >: A, That](b: Traversable[(K, B)])(
        implicit bf: CanBuildFrom[Repr, (K, B), That]): That = {
      val occ = b.map(_._1).toSet
      val buf = bf()
      a.foreach { case (key, value) =>
        if (!(occ contains key))
          buf += key -> value
      }
      buf.result
    }
  }

  implicit class IntersectUnionByPimp[A](a: Traversable[A]) {
    /**
     * Intersect two collections by their keys, with separate key-selection
     * functions for the two collections. This is identical to
     * `intersectBy` except that each collection has its own key-selection
     * function. This allows the types of the two collections to be
     * distinct, with no common parent.
     *
     * @param b Other collection to intersect with.
     * @param key1fn Function to select the comparison key for the first
     *   collection.
     * @param key1fn Function to select the comparison key for the first
     *   collection.
     * @param combine Function to combine values from the two collections.
     */
    def intersectBy2[K, B, R](b: Traversable[B])(key1fn: A => K
        )(key2fn: B => K)(combine: (A, B) => R): Traversable[R] = {
      val keyed_a = a.map { x => (key1fn(x), x) }
      val keyed_b = b.map { x => (key2fn(x), x) }
      keyed_a.intersectWith(keyed_b)(combine).map(_._2)
    }

    /**
     * Intersect two collections by their keys. Keep the ordering of
     * objects in the first collection. Use a combiner function to
     * combine items in common. If either item is a multi-map, then
     * for a key seen `n` times in the first collection and `m`
     * times in the second collection, it will occur `n * m` times
     * in the resulting collection, including all the possible
     * combinations of pairs of identical keys in the two collections.
     *
     * @param b Other collection to intersect with.
     * @param keyfn Function to select the comparison key.
     * @param combine Function to combine values from the two collections.
     */
    def intersectBy[K, B >: A](b: Traversable[B])(keyfn: B => K)(
        combine: (A, B) => B): Traversable[B] = {
      val keyed_a = a.map { x => (keyfn(x), x) }
      val keyed_b = b.map { x => (keyfn(x), x) }
      keyed_a.intersectWith(keyed_b)(combine).map(_._2)
    }

    /**
     * Union two collections by their keys. Keep the ordering of
     * objects in the first collection, followed by the second
     * collection. Use a combiner function to combine items in common.
     * If either item is a multi-map, then for a key seen `n` times
     * in the first collection and `m` times in the second collection,
     * it will occur `n * m` times in the resulting collection, including
     * all the possible combinations of pairs of identical keys in the
     * two collections.
     *
     * @param b Other collection to union with.
     * @param keyfn Function to select the comparison key.
     * @param combine Function to combine values from the two collections.
     */
    def unionBy[K, B >: A](b: Traversable[B])(keyfn: B => K)(
        combine: (A, B) => B): Traversable[B] = {
      val keyed_a = a.map { x => (keyfn(x), x) }
      val keyed_b = b.map { x => (keyfn(x), x) }
      keyed_a.unionWith(keyed_b)(combine).map(_._2)
    }

    /**
     * Difference two collections by their keys. Keep the ordering of
     * objects in the first collection.
     *
     * FIXME: This doesn't quite work the way that plain `diff` works.
     * In our case, if a key occurs in `b`, all elements with the same
     * key in `a` are removed. In plain `diff`, only as many elements
     * are removed from `a` as occur in `b`. This makes sense with
     * multisets for simple objects but it's less clear whether this
     * makes sense with keys, because the values will typically be
     * different.
     *
     * @param b Other collection to difference with.
     * @param keyfn Function to select the comparison key.
     */
    def diffBy[K, B >: A](b: Traversable[B])(keyfn: B => K
        ): Traversable[B] = {
      val keyed_a = a.map { x => (keyfn(x), x) }
      val keyed_b = b.map { x => (keyfn(x), x) }
      keyed_a.diffWith(keyed_b).map(_._2)
    }
  }

 /////////////////////////////////////////////////////////////////////////////
 //                        Misc. list/iterator functions                    //
 /////////////////////////////////////////////////////////////////////////////

  def fromto(from: Int, too: Int) = {
    if (from <= too) (from to too)
    else (too to from)
  }

  /**
   * Return true if sequence is sorted.
   */
  def is_sorted[T: Ordering](l: Iterable[T]) =
    l.view.zip(l.tail).forall(x => implicitly[Ordering[T]].lteq(x._1, x._2))

  /**
   * Return true if sequence is reverse-sorted.
   */
  def is_reverse_sorted[T: Ordering](l: Iterable[T]) =
    l.view.zip(l.tail).forall(x => implicitly[Ordering[T]].gteq(x._1, x._2))

  // Return an iterator over all elements in all the given sequences, omitting
  // elements seen more than once and keeping the order.
  def merge_numbered_sequences_uniquely[A, B](seqs: Iterable[(A, B)]*) = {
    val keys_seen = mutable.Set[A]()
    for {
      seq <- seqs
      (s, vall) <- seq
      if (!(keys_seen contains s))
    } yield {
      keys_seen += s
      (s, vall)
    }
  }

  /**
   * Combine two maps, adding up the numbers where overlap occurs.
   */
  def combine_maps[T, Num](map1: Map[T, Num], map2: Map[T, Num]
      )(implicit num: Numeric[Num]) = {
    /* We need to iterate over one of the maps and add each element to the
       other map, checking first to see if it already exists.  Make sure
       to iterate over the smallest map, so that repeated combination of
       maps will have O(n) rather than worst-case O(N^2). */
    if (map1.size > map2.size)
      map1 ++ map2.map {
        case (k,v) => k -> num.plus(v, map1.getOrElse(k,num.zero))
      }
    else
      map2 ++ map1.map {
        case (k,v) => k -> num.plus(v, map2.getOrElse(k,num.zero))
      }
  }

  /**
   * Convert a list of items to a map counting how many of each item occurs.
   */
  def list_to_item_count_map[T : Ordering](list: Seq[T]) =
    list.sorted groupBy identity mapValues (_.size)

  implicit def to_CollectionTravOncePimp[T](iable: TraversableOnce[T]) =
  new {
    def countItems: BaseMap[T, Int] = {
      val counts = intmap[T]()
      iable.foreach { counts(_) += 1 }
      counts
    }
  }

  implicit def to_CollectionNumericPartOrdSeqPimp[T, U : Ordering](
    seq: Seq[(T, U)]
  ) = new {
    private val ordering = implicitly[Ordering[U]]
    def sortNumeric = seq sortWith { (x,y) => ordering.lt(x._2, y._2) }
    def sortNumericRev = seq sortWith { (x,y) => ordering.gt(x._2, y._2) }
  }

  implicit def wrappedArrayToIndexedSeq[T](buf: mutable.WrappedArray[T]) =
    buf.toIndexedSeq

  implicit def bufferToIndexedSeq[T](buf: mutable.Buffer[T]) = buf.toIndexedSeq
}

package object collection extends CollectionPackage { }
