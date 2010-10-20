///////////////////////////////////////////////////////////////////////////////
//  Copyright (C) 2010 Travis Brown, The University of Texas at Austin
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
package opennlp.textgrounder.topo.util

import java.io._
import java.io.InputStream
import scala.collection.mutable.Map

class CodeConverter(in: InputStream) {
  def this() = this {
    getClass.getResourceAsStream("/data/geo/country-codes.txt")
  }

  case class Country(
    name: String,
    fips: Option[String],
    iso:  Option[(String, String, Int)],
    stanag: Option[String],
    tld: Option[String])

  private val countriesF = Map[String, Country]()
  private val countriesI = Map[String, Country]()
  private val reader = new BufferedReader(new InputStreamReader(in))

  private var line = reader.readLine
  while (line != null) {
    val fs = line.split("\t")
    val country = Country(
      fields(0),
      if (fs(1) == "-") None else Some(fs(1)),
      if (fs(2) == "-") None else Some(fs(2), fs(3), fs(4).toInt),
      if (fs(5) == "-") None else Some(fs(5)),
      if (fs(6) == "-") None else Some(fs(6))
    )
    country.fips match {
      case Some(fips) => countriesF(fips) = country
      case _ =>
    }
    country.iso match {
      case Some((iso2, _, _)) => countriesI(iso2) = country
      case _ =>
    }
    line = reader.readLine
  }

  def convertFipsToIso2(code: String): Option[String] =
    countriesF.get(code).flatMap(_.iso.map(_._1))

  def convertIso2ToFips(code: String): Option[String] =
    countriesI.get(code).flatMap(_.fips)
}

object CodeConverter {
  def main(args: Array[String]) {
    val converter = new CodeConverter()
    println(args(0) match {
      case "f2i" => converter.convertIso2ToFips(args(1))
      case "i2f" => converter.convertFipsToIso2(args(1))
    })
  }
}

