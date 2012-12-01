///////////////////////////////////////////////////////////////////////////////
//  MeteredTask.scala
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

package opennlp.textgrounder
package util

import scala.util.control.Breaks._

import collectionutil.{InterruptibleIterator, SideEffectIterator}
import osutil._
import printutil.errprint
import textutil._
import timeutil.format_minutes_seconds

/////////////////////////////////////////////////////////////////////////////
//                             Metered Tasks                               //
/////////////////////////////////////////////////////////////////////////////

/**
 * Class for tracking number of items processed in a long task, and
 * reporting periodic status messages concerning how many items
 * processed and how much real time involved.  Use in any of the following
 * ways:
 *
 * (1) Call `start` to begin processing, `item_processed` every time you have
 *     processed an item, and `finish` to end processing.
 * (1) Call `foreach` to process each item in a Traversable (either an
 *     Iterable or Iterator), automatically calling `start, `item_processed`
 *     and `end` as appropriate so that progress messages are output.
 * (3) Use `iterate` to wrap an iterator in such a way that progress messages
 *     will be output as items are fetched from the iterator.  This ensures
 *     that `start` and `finish` are executed when iteration starts and ends,
 *     not immediately -- important since the iterator may not actually be
 *     processed until some indefinite time in the future.
 *
 * @param item_name Generic name of the items being processed, for the
 *    status messages.
 * @param verb Transitive verb in its -ing form indicating what is being
 *    done to the items.
 * @param secs_between_output Number of elapsed seconds between successive
 *    periodic status messages.
 * @param maxtime Maximum amount of time allowed for execution, in seconds.
 *    If 0.0 (the default), no maximum. This affects the return value from
 *    `item_processed`, which will be `true` if further processing should
 *    stop. This will be respected by `foreach` and `iterate`, which will
 *    stop further processing.
 * @param maxitems Maximum number of items allowed to be processed.  If 0
 *    (the default), no maximum. This works similarly to `maxtime`.
 */
class MeteredTask(item_name: String, verb: String,
    secs_between_output: Double = 15, maxtime: Double = 0.0,
    maxitems: Int = 0) {
  val plural_item_name = pluralize(item_name)
  protected var items_processed = 0
  // Whether we've already printed stats after the most recent item
  // processed
  protected var printed_stats = false
  protected var started = false
  protected var first_time: Double = _
  protected var last_time: Double = _

  def elapsed_time = curtimesecs - first_time
  def num_processed = items_processed

  def item_unit = {
    if (items_processed == 1)
      item_name
    else
      plural_item_name
  }

  def start() = {
    errprint("--------------------------------------------------------")
    first_time = curtimesecs
    last_time = first_time
    errprint("Beginning %s %s at %s.", verb, plural_item_name,
      humandate_full(first_time))
    errprint("")
    started = true
    this
  }

  def print_elapsed_time_and_rate(curtime: Double = curtimesecs,
      include_timestamp: Boolean = true) {
    /* Don't do anything if already printed for this item. */
    if (printed_stats)
      return
    printed_stats = true
    val total_elapsed_secs = curtime - first_time
    val attime =
      if (!include_timestamp) "" else "At %s: " format humandate_time(curtime)
    errprint("%sElapsed time: %s, %s %s processed",
             attime,
             format_minutes_seconds(total_elapsed_secs, hours=false),
             items_processed, item_unit)
    val items_per_second = items_processed.toDouble / total_elapsed_secs
    val seconds_per_item = total_elapsed_secs / items_processed
    errprint("Processing rate: %s items per second (%s seconds per item)",
             format_float(items_per_second),
             format_float(seconds_per_item))
  }

  /**
   * Indicate that an item has been processed.  Return value is `true` if
   * processing should stop (maximum time exceeded), `false` otherwise.
   */
  def item_processed() = {
    assert(started)
    val curtime = curtimesecs
    items_processed += 1
    val total_elapsed_secs = curtime - first_time
    val last_elapsed_secs = curtime - last_time
     printed_stats = false
    if (last_elapsed_secs >= secs_between_output) {
      // Rather than directly recording the time, round it down to the
      // nearest multiple of secs_between_output; else we will eventually
      // see something like 0, 15, 45, 60, 76, 91, 107, 122, ...
      // rather than like 0, 15, 45, 60, 76, 90, 106, 120, ...
      val rounded_elapsed =
        ((total_elapsed_secs / secs_between_output).toInt *
         secs_between_output)
      last_time = first_time + rounded_elapsed
      print_elapsed_time_and_rate(curtime)
    }
    if (maxtime > 0 && total_elapsed_secs >= maxtime) {
      errprint("Stopping processing because maximum time %s reached",
        format_minutes_seconds(maxtime, hours=false))
      print_elapsed_time_and_rate(curtime)
      true
    } else if (maxitems > 0 && items_processed >= maxitems) {
      errprint("Stopping processing because limit of %s items reached",
        maxitems)
      print_elapsed_time_and_rate(curtime)
      true
    } else false
  }

  /**
   * Finish metering.
   *
   * This outputs a message indicating that processing is finished, along with
   * stats giving number of items processed, time, and items/time, time/item.
   * The total message looks like "Finished _doing_ _items_." where "doing"
   * comes from the `doing` parameter to this function and should be a
   * lower-case transitive verb in the -ing form.  The actual value of
   * "items" comes from the `item_name` constructor parameter to this
   * class. */ 
  def finish() {
    val curtime = curtimesecs
    errprint("")
    errprint("Finished %s %s at %s.", verb, plural_item_name,
      humandate_full(curtime))
    // Don't include timestamp since we already output it.
    print_elapsed_time_and_rate(curtime, include_timestamp = false)
    output_resource_usage()
    errprint("--------------------------------------------------------")
  }

  def foreach[T](trav: Traversable[T])(f: T => Unit) {
    start()
    breakable {
      trav.foreach {
        x => f(x)
        if (item_processed())
          break
      }
    }
    finish()
  }

  def iterate[T](iter: Iterator[T]) = {
    val wrapiter = new InterruptibleIterator(iter)
    new SideEffectIterator { start() } ++
    wrapiter.map { x => 
      if (item_processed())
        wrapiter.stop()
      x
    } ++
    new SideEffectIterator { finish() }
  }
}
