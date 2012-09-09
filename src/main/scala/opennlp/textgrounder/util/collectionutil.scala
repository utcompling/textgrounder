///////////////////////////////////////////////////////////////////////////////
//  collectiontuil.scala
//
//  Copyright (C) 2011 Ben Wing, The University of Texas at Austin
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
//  Copyright (C) 2011 Ben Wing, The University of Texas at Austin
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

package opennlp.textgrounder.util

import scala.util.control.Breaks._
import scala.collection.mutable
import scala.collection.mutable.{Builder, MapBuilder}
import scala.collection.generic.CanBuildFrom

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

package object collectionutil {

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
  class DynamicArray[@specialized T: ClassManifest](initial_alloc: Int = 8) {
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
           val collector = items_by_range.getOrElse(lower, null.asInstanceOf[Coll])
           if (collector != null || unseen_all ||
               (unseen_between && seen_any &&
                upper != max_value && upper <= highest_seen))
           val col2 = if (collector != null) collector else create(lower)
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
 //                        Misc. list/iterator functions                    //
 /////////////////////////////////////////////////////////////////////////////

  def fromto(from: Int, too: Int) = {
    if (from <= too) (from to too)
    else (too to from)
  }

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
  def combine_maps[T, U <: Int](map1: Map[T, U], map2: Map[T, U]) = {
      /* We need to iterate over one of the maps and add each element to the
         other map, checking first to see if it already exists.  Make sure
         to iterate over the smallest map, so that repeated combination of
         maps will have O(n) rather than worst-case O(N^2). */
      if (map1.size > map2.size)
        map1 ++ map2.map { case (k,v) => k -> (v + map1.getOrElse(k,0)) }
      else
        map2 ++ map1.map { case (k,v) => k -> (v + map2.getOrElse(k,0)) }
    }

  /**
   * Convert a list of items to a map counting how many of each item occurs.
   */
  def list_to_item_count_map[T : Ordering](list: Seq[T]) =
    list.sorted groupBy identity mapValues (_.size)
}

