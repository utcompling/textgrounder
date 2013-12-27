///////////////////////////////////////////////////////////////////////////////
//  metering.scala
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

package opennlp.textgrounder
package util

import scala.util.control.Breaks._
import scala.collection.{GenTraversable, GenTraversableOnce, GenTraversableLike}
import scala.collection.generic.CanBuildFrom

import collection.{InterruptibleIterator, SideEffectIterator}
import os._
import print.errprint
import text._
import time._

/////////////////////////////////////////////////////////////////////////////
//                             Metered Tasks                               //
/////////////////////////////////////////////////////////////////////////////

protected class MeteringPackage {
  protected var started = false
  protected var first_time: Double = _
  protected var last_time: Double = _

  class Timer(action: String) {
    def start() {
      errprint("--------------------------------------------------------")
      first_time = curtimesecs
      last_time = first_time
      errprint("Beginning %s at %s.", action, humandate_full(first_time))
      started = true
    }

    def print_elapsed_time(curtime: Double) {
      val total_elapsed_secs = curtime - first_time
      errprint("Elapsed time: %s (since start: %s)",
               format_minutes_seconds(total_elapsed_secs,
                 include_hours = false),
               format_minutes_seconds(get_program_time_usage))
    }

    def finish() {
      val curtime = curtimesecs
      errprint("Finished %s at %s.", action, humandate_full(curtime))
      // Don't include timestamp since we already output it.
      print_elapsed_time(curtime)
      output_resource_usage(omit_time = true)
    }
  }

  def time_action[T](action: String)(fn: => T) = {
    {
      val timer = new Timer(action)
      timer.start()
      val retval = fn
      timer.finish()
      retval
    }
  }

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
   * @param verb Transitive verb in its -ing form indicating what is being
   *    done to the items being processed; used for status messages.  If there
   *    is a %s in the text, the item name will be substituted, else appended.
   * @param item_name Generic name of the items being processed; used for
   *    status messages.
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
  class Meter(verb: String, item_name: String,
      secs_between_output: Double = 15, maxtime: Double = 0.0,
      maxitems: Int = 0) {
    val plural_item_name = pluralize(item_name)
    protected var items_processed = 0
    // Whether we've already printed stats after the most recent item
    // processed
    protected var printed_stats = false

    def elapsed_time = curtimesecs - first_time
    def num_processed = items_processed

    // total elapsed secs, items per second, seconds per item
    def tes_ips_spi = {
      val total_elapsed_secs = elapsed_time
      (total_elapsed_secs,
        items_processed.toDouble / total_elapsed_secs,
        total_elapsed_secs / items_processed)
    }

    class MeterTimer(action: String) extends Timer(action) {
      override def print_elapsed_time(curtime: Double) {
        // Already printed timestamp.
        print_elapsed_time_and_rate(curtime, include_timestamp = false)
      }
    }

    val meter_timer = new MeterTimer(construct_predicate)

    def item_unit = {
      if (items_processed == 1)
        item_name
      else
        plural_item_name
    }

    protected def construct_predicate = {
      val plurit = plural_item_name
      if (verb contains "%s")
        verb.replace("%s", plurit)
      else
        verb + " " + plurit
    }

    def start() = {
      meter_timer.start()
      this
    }

    def print_elapsed_time_and_rate(curtime: Double = curtimesecs,
        include_timestamp: Boolean = true) {
      /* Don't do anything if already printed for this item. */
      if (printed_stats)
        return
      printed_stats = true
      val (tes, ips, spi) = tes_ips_spi
      val attime = if (!include_timestamp) "" else
        "At %s: " format humandate_time(curtime)
      errprint("%sElapsed time: %s (since start: %s)",
               attime,
               format_minutes_seconds(tes, include_hours = false),
               format_minutes_seconds(get_program_time_usage))
      errprint("%s %s processed, %s items/sec, %s sec/item",
        items_processed, item_unit, format_double(ips), format_double(spi))
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
          format_minutes_seconds(maxtime,
            include_hours = false))
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
      meter_timer.finish()
    }

    def foreach[T, Repr](trav: GenTraversableLike[T, Repr])(f: T => Unit) {
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
      new SideEffectIterator({ start() }) ++
      wrapiter.map { x => 
        if (item_processed())
          wrapiter.stop()
        x
      } ++
      new SideEffectIterator({ finish() })
    }
  }

  implicit class MeteredGenTraversablePimp[T, Repr](
      trav: GenTraversableLike[T, Repr]
    ) {
    def foreachMetered(m: Meter)(f: T => Unit) =
      m.foreach(trav)(f)

    def mapMetered[B, That](m: Meter)(f: T => B)(
      implicit bf: CanBuildFrom[Repr, B, That]
    ): That = {
      m.start()
      try {
        trav.map { x =>
          val z = f(x)
          m.item_processed()
          z
        } (bf)
      } finally { m.finish() }
    }

    def flatMapMetered[B, That](m: Meter)(f: (T) => GenTraversableOnce[B])(
      implicit bf: CanBuildFrom[Repr, B, That]
    ): That = {
      m.start()
      try {
        trav.flatMap { x =>
          val z = f(x)
          m.item_processed()
          z
        } (bf)
      } finally { m.finish() }
    }
  }

  implicit class MeteredIteratorPimp[T](iter: Iterator[T]) {
    def mapMetered[B](m: Meter)(f: (T) => B): Iterator[B] =
      m.iterate(iter.map(f))

    def flatMapMetered[B](m: Meter)(f: (T) => GenTraversableOnce[B]
      ): Iterator[B] = m.iterate(iter.flatMap(f))
  }
}

package object metering extends MeteringPackage { }
