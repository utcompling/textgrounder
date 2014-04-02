///////////////////////////////////////////////////////////////////////////////
//  table.scala
//
//  Copyright (C) 2011-2014 Ben Wing, The University of Texas at Austin
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

import java.io.PrintStream

////////////////////////////////////////////////////////////////////////////
//                           Table-output functions                       //
////////////////////////////////////////////////////////////////////////////

package object table {

  /**
   * Return the printf-style format for formatting a table consisting of a
   * series of lines, each of which contains a series of columns. First
   * computes the maximum size of each column and then formats appropriately
   * so everything fits.
   *
   * @param lines Lines of column values to format.
   * @param maxcolsize Maximum size of a column.
   */
  def table_column_format(lines: Iterable[Iterable[String]],
      maxcolsize: Int = 40) = {
    val ncols = lines.head.size
    // Make sure all lines have same number of columns
    lines foreach { line => assert(line.size == ncols) }
    val maxsize = lines.transpose.map { line =>
      line.map { _.length}.max min maxcolsize
    }
    maxsize.map { size => s"%-${size}s" } mkString " "
  }

  /**
   * Format a table consisting of a series of lines, each of which contains
   * a series of columns. First computes the maximum size of each column
   * and then formats appropriately so everything fits.
   *
   * @param lines Lines of column values to format.
   * @param maxcolsize Maximum size of a column.
   */
  def format_table(lines: Iterable[Iterable[String]], maxcolsize: Int = 40) = {
    val fmt = table_column_format(lines, maxcolsize)
    lines.map { line => fmt.format(line.toSeq: _*) } mkString "\n"
  }

  /**
   * Format a table consisting of a series of lines, each of which contains
   * a series of columns. The first line is a set of column headers.
   * First computes the maximum size of each column and then formats
   * appropriately so everything fits. Outputs a line of dashes between
   * header and remaining lines.
   *
   * @param headers Column headers for columns to format.
   * @param lines Lines of column values to format.
   * @param maxcolsize Maximum size of a column.
   */
  def format_table_with_header(headers: Iterable[String],
      lines: Iterable[Iterable[String]], maxcolsize: Int = 40) = {
    val fmt = table_column_format(headers +: lines.toSeq)
    val header_line = fmt.format(headers.toSeq: _*)
    (Iterable(header_line, "-" * header_line.size) ++
      lines.map { line => fmt.format(line.toSeq: _*) }) mkString "\n"
  }

  /**
   * Given a list of tuples, output the list, one line per tuple.
   *
   * @param outfile If specified, send output to this stream instead of
   *   stdout.
   * @param indent If specified, indent all rows by this string (usually
   *   some number of spaces).
   * @param maxrows If specified, output at most this many rows.
   */
  def output_tuple_list[T,U](
      items: Seq[(T,U)], outfile: PrintStream = System.out,
      indent: String = "", maxrows: Int = -1) {
    var its = items
    if (maxrows >= 0)
      its = its.slice(0, maxrows)
    for ((key, value) <- its)
      outfile.println("%s%s = %s" format (indent, key, value))
  }

  /**
   * Given a list of tuples, where the second element of the tuple is a
   * number and the first a key, output the list, sorted on the numbers from
   * bigger to smaller.  Within a given number, normally sort the items
   * alphabetically.
   *
   * @param keep_secondary_order If true, the original order of items is
   *   left instead of sorting secondarily.
   * @param outfile If specified, send output to this stream instead of
   *   stdout.
   * @param indent If specified, indent all rows by this string (usually
   *   some number of spaces).
   * @param maxrows If specified, output at most this many rows.
   */
  def output_reverse_sorted_list[T <% Ordered[T],U <% Ordered[U]](
      items: Seq[(T,U)], keep_secondary_order: Boolean = false,
      outfile: PrintStream = System.out, indent: String = "",
      maxrows: Int = -1) {
    var its = items
    if (!keep_secondary_order)
      its = its sortBy (_._1)
    its = its sortWith (_._2 > _._2)
    output_tuple_list(its, outfile, indent, maxrows)
  }

  /**
   * Given a table with values that are numbers, output the table, sorted on
   * the numbers from bigger to smaller.  Within a given number, normally
   * sort the items alphabetically.
   *
   * @param keep_secondary_order If true, the original order of items is
   *   left instead of sorting secondarily.
   * @param outfile If specified, send output to this stream instead of
   *   stdout.
   * @param indent If specified, indent all rows by this string (usually
   *   some number of spaces).
   * @param maxrows If specified, output at most this many rows.
   */
  def output_reverse_sorted_table[T <% Ordered[T],U <% Ordered[U]](
      table: scala.collection.Map[T,U], keep_secondary_order: Boolean = false,
      outfile: PrintStream = System.out, indent: String = "",
      maxrows: Int = -1) {
    output_reverse_sorted_list(table toList, keep_secondary_order,
      outfile, indent, maxrows)
  }

  /**
   * Output a table, sorted by its key.
   *
   * @param outfile If specified, send output to this stream instead of
   *   stdout.
   * @param indent If specified, indent all rows by this string (usually
   *   some number of spaces).
   * @param maxrows If specified, output at most this many rows.
   */
  def output_key_sorted_table[T <% Ordered[T],U](
      table: scala.collection.Map[T,U],
      outfile: PrintStream = System.out, indent: String = "",
      maxrows: Int = -1) {
    output_tuple_list(table.toSeq.sortBy (_._1), outfile, indent,
      maxrows)
  }
}
