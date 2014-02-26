///////////////////////////////////////////////////////////////////////////////
//  Morans.scala
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
package geolocate

import util.argparser._
import util.experiment._
import langmodel.Unigram
import util.textdb.TextDB

import scala.collection.mutable.Map

import java.io._

class MoransParameters(
  parser: ArgParser
) extends GeolocateParameters(parser) {
  var morans_output =
  //NEED TO REDO COMMENTING ON MORANS, NOT CORRECT
    ap.option[String]("mo", "morans_output",
      metavar = "String",
      default = "MoransOutput.txt",
      help = """File name of output file""")
}

class MoransDriver extends
    GeolocateDriver with StandaloneExperimentDriverStats {
  type TParam = MoransParameters
  type TRunRes = Unit

  /**
   * Generate the salience grid.
   */

  // def CalcMoransWord(word:String, wordFreqsRef:File, GridReference:File){
  //
  // }

  def run() {
    //First Create Grid
    val xgrid = initialize_grid
    val grid = xgrid match {
      case g: MultiRegularGrid => g
      case _ => throw new IllegalArgumentException("Morans needs a regular grid, not a K-d grid")
    }
    println("######Starting Morans#######")
    val wordMoransMap = Map[Int, Double]()
    val wordMoransSum_Numer = Map[Int, Double]()
    val wordMoransSum_Denom = Map[Int, Double]()
    val wordMoransSum_Neighbors = Map[Int, Double]()

    //println(grid.asInstanceOf[AnyRef].getClass.getSimpleName)
    val wordMap_lookupMap = getWordMeans(grid)

    for (xcell <- grid.iter_nonempty_cells) {
      val cell = xcell.asInstanceOf[MultiRegularCell]
      //println("Cell: %s" format cell)
      /*println("#############")
      println(cell.index.latind)
      println(cell.index.longind)
      println("#############")*/

      //println(cell.asInstanceOf[AnyRef].getClass.getSimpleName)
      val lm = Unigram.check_unigram_lang_model(cell.lang_model.grid_lm)

      val index = cell.index
      //New way of calculating Morans.  Every cell that contains any data will have a probability for every word. Every cell can be the focus of a window
      for (key <- wordMap_lookupMap._2){

        val word = key._1
        //println(key._2)
        val mean = wordMap_lookupMap._1(word)

        val CAF = cellProb((index.latind + 1), index.longind, grid, word)
        //println("Cell Above Prob " + CAF)

        //Cell Below Frequency (CBF)
        val CBF = cellProb((index.latind - 1), index.longind, grid, word)

        //val CBF = cellProb((cell.index.latind - 1), cell.index.longind, grid, word)
        //println("Cell Below Prob " + CBF)

        //Cell Left Frequency (CLF)
        val CLF = cellProb(index.latind, (index.longind - 1) , grid, word)
        //val CLF = cellProb(cell.index.latind, (cell.index.longind - 1), grid, word)
        //println("Cell Left Prob " + CLF)

        //Cell Right Frequency (CRF)
        val CRF = cellProb(index.latind, (index.longind + 1), grid, word)
        //val CRF = cellProb(cell.index.latind, (cell.index.longind + 1), grid, word)
        //println("Cell Right Prob " + CRF)

        //Cell Frequency (CF)
        val CF = cellProb(index.latind, index.longind, grid, word)
        //val CF = cellProb(cell.index.latind, cell.index.longind, grid, word)
        //println("Cell Center Prob " + CF)

        val morans_numerator = CalcNumer(CF, CAF, CBF, CLF, CRF, mean)._1

        //Total_Numerator_Word = Total_Numerator_Word + Morans_Numerator
        if (wordMoransSum_Numer.contains(word)){
          wordMoransSum_Numer(word) += morans_numerator
          //println(wordMoransSum_Numer(word))
        }else{
          wordMoransSum_Numer += (word -> morans_numerator)
          //println(wordMoransSum_Numer(word))
        }

        //Num_of_Neighbors_with_word (Adjacent neighbors)
        val num_of_neighbors_with_word = (4.0 - CalcNumer(CF, CAF, CBF, CLF, CRF, mean)._2)

        //Normalizing_Factor_Word = (CF - Mean)^2
        val morans_denom = ((CF - mean)*(CF - mean))

        //Check Calculation for "yalll"
        /*if (word == 12955){
          println("#########")
          println("CF: %s, CAF: %s, CBF: %s, CLF: %s, CRF: %s, Mean: %s, Morans_Num: %s, Morans_Denom: %s, Neighbors: %s" format (CF, CAF, CBF, CLF, CRF, mean, morans_numerator, morans_denom, num_of_neighbors_with_word))
        }*/

        //Total_Denom_Word = Total_Denom_Word + Normalizing_Factor_Word
        if (wordMoransSum_Denom.contains(word)){
          wordMoransSum_Denom(word) += (morans_denom / (wordMap_lookupMap._3(word).toDouble))
        }else{
          wordMoransSum_Denom += (word -> (morans_denom / (wordMap_lookupMap._3(word).toDouble)))
        }


        //Total_Neighbors_word = Total_Neighbors_word + Num_of_Neighbors_with_word
        if (wordMoransSum_Neighbors.contains(word)){
          wordMoransSum_Neighbors(word) += num_of_neighbors_with_word
        }else{
          wordMoransSum_Neighbors += (word -> num_of_neighbors_with_word)
        }

      }
    }

      //This section marks the old way of calculating Morans.  Assumes that neighbors that lack a word have a probability of zero.
      //Also assumes only cells that contain the word can be the focus of a  window
      /*for ((word, _) <- lm.iter_grams){
        //Word Global Mean Frequency

        val mean = wordMap_lookupMap._1(word)

        //println(wordMap_lookupMap._2(word))

        val index = cell.index
        //Cell Above Frequency (CAF)
        val CAF = cellProb((index.latind + 1), index.longind, grid, word)
        //println(CAF)

        //Cell Below Frequency (CBF)
        val CBF = cellProb((cell.index.latind - 1), cell.index.longind, grid, word)
        //println(CBF)

        //Cell Left Frequency (CLF)
        val CLF = cellProb(cell.index.latind, (cell.index.longind - 1), grid, word)
        //println(CLF)

        //Cell Right Frequency (CRF)
        val CRF = cellProb(cell.index.latind, (cell.index.longind + 1), grid, word)
        //println(CRF)

        //Cell Frequency (CF)
        val CF = cellProb(cell.index.latind, cell.index.longind, grid, word)
        //println(CF)

        //Morans_Numerator (CF - Mean)*(CAF - Mean) + (CF - Mean)*(CBF - Mean) + (CF - Mean)*(CLF-Mean) + (CF-Mean)*(CRF-Mean)
        //println(wordMap_lookupMap._2(word))
        val morans_numerator = CalcNumer(CF, CAF, CBF, CLF, CRF, mean)._1

        //Total_Numerator_Word = Total_Numerator_Word + Morans_Numerator
        if (wordMoransSum_Numer.contains(word)){
          wordMoransSum_Numer(word) += morans_numerator
          //println(wordMoransSum_Numer(word))
        }else{
          wordMoransSum_Numer += (word -> morans_numerator)
          //println(wordMoransSum_Numer(word))
        }

        //Num_of_Neighbors_with_word (Adjacent neighbors)
        val num_of_neighbors_with_word = (4.0 - CalcNumer(CF, CAF, CBF, CLF, CRF, mean)._2)

        //Normalizing_Factor_Word = (CF - Mean)^2
        val morans_denom = ((CF - mean)*(CF - mean))

        //Check Calculation for "yalll"
        /*if (word == 12955){
          println("#########")
          println("CF: %s, CAF: %s, CBF: %s, CLF: %s, CRF: %s, Mean: %s, Morans_Num: %s, Morans_Denom: %s, Neighbors: %s" format (CF, CAF, CBF, CLF, CRF, mean, morans_numerator, morans_denom, num_of_neighbors_with_word))
        }*/

        //Total_Denom_Word = Total_Denom_Word + Normalizing_Factor_Word
        if (wordMoransSum_Denom.contains(word)){
          wordMoransSum_Denom(word) += (morans_denom / (wordMap_lookupMap._3(word).toDouble))
        }else{
          wordMoransSum_Denom += (word -> (morans_denom / (wordMap_lookupMap._3(word).toDouble)))
        }


        //Total_Neighbors_word = Total_Neighbors_word + Num_of_Neighbors_with_word
        if (wordMoransSum_Neighbors.contains(word)){
          wordMoransSum_Neighbors(word) += num_of_neighbors_with_word
        }else{
          wordMoransSum_Neighbors += (word -> num_of_neighbors_with_word)
        }
      }
        //println("Word: %s, count: %s" format (lm.gram_to_string(word), count))
    }*/
    //val dir = new File()
    //val fw = new FileWriter(dir.getAbsoluteFile())

    println("#####Starting to Write Out#######")
    //Write Out Info to file
    val bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(params.morans_output), "UTF-8"))
    bw.write(String.format("WordID|Word|WordAppears|MC%n"))
    for (thing <- wordMap_lookupMap._1 ){
      //Currently Morans Coef only gets results for words that have at least 1 neighbor with the word
      wordMoransMap += (thing._1 -> (((wordMoransSum_Numer(thing._1) / wordMoransSum_Neighbors(thing._1))  / (wordMoransSum_Denom(thing._1)))))

      /*if (thing._1 == 12955){
        println("WordID: %s, Word: %s, MC: %s" format (thing._1, wordMap_lookupMap._2(thing._1) , wordMoransMap(thing._1)))
        println(wordMoransSum_Numer(thing._1))
        println(wordMoransSum_Neighbors(thing._1))
        println(wordMoransSum_Denom(thing._1))
      }*/
      //wordMoransMap(thing._1)
      if (wordMap_lookupMap._3(thing._1) > 15){
        println("WordID: %s, Word: %s, WordAppears: %s, MC: %s" format (thing._1, wordMap_lookupMap._2(thing._1) , wordMap_lookupMap._3(thing._1) ,wordMoransMap(thing._1)))
        //println(wordMoransSum_Numer(thing._1))
        //println(wordMoransSum_Neighbors(thing._1))
        //println(wordMoransSum_Denom(thing._1))
        bw.write( thing._1 + "|" + wordMap_lookupMap._2(thing._1) + "|" + wordMap_lookupMap._3(thing._1) + "|" + wordMoransMap(thing._1) + String.format("%n"))
        //println((grid.num_nonempty_cells).toDouble)
      }
    }
    bw.close()
  }
  def cellProb(latind:Int, longind:Int, grid:MultiRegularGrid, word:Int) = {
    //Check below line for possible problem
    val cell = grid.find_cell_for_cell_index(RegularCellIndex(grid, latind, longind), create=false, record_created_cell=false)
    val CF = cell match {
      case Some(cell) => Unigram.check_unigram_lang_model(cell.lang_model.grid_lm).gram_prob(word)
      case None => -99.9
    }
    CF
  }

  def CalcNumer(CF:Double, CAF:Double, CBF:Double, CLF:Double, CRF:Double, mean:Double) = {
    var sum = 0.0
    var null_neighbor = 0.0
    for (arg <- List(CAF, CBF, CLF, CRF)){
      if (arg != -99.9){
        sum += (arg - mean)*(CF - mean)
        //println("Arg: %s , mean: %s , CF: %s , Product: %s" format (arg, mean, CF, (arg - mean)*(CF - mean)))
      }else{
        null_neighbor += 1.0
      }
    }
    //println(sum, null_neighbor)
    (sum, null_neighbor)
  }

  def getWordMeans(grid:MultiRegularGrid) = {
    var j = 0.0
    val wordMeanMap = Map[Int, Double]()
    val wordTotalProb = Map[Int, Double]()
    val idToWordMap = Map[Int, String]()
    val wordAppears = Map[Int, Int]()
    for (cell <- grid.iter_nonempty_cells){
      j = j + 1.0
      val lm = Unigram.check_unigram_lang_model(cell.lang_model.grid_lm)
      for ((word, _) <- lm.iter_grams){
        if (wordTotalProb.contains(word)){
          wordTotalProb(word) += lm.gram_prob(word)
        }
        else{
          wordTotalProb += (word -> lm.gram_prob(word))
          idToWordMap += (word -> lm.gram_to_string(word))
        }
        if (wordAppears.contains(word)){
          wordAppears(word) += 1
        }
        else{
          wordAppears += (word -> 1)
        }
      }
    }
    for ((key, value) <- wordTotalProb){
      wordMeanMap += (key -> (wordTotalProb(key).toDouble/(wordAppears(key).toDouble)))
    }
    (wordMeanMap, idToWordMap, wordAppears)
  }

}

object MoransApp extends GeolocateApp("morans") {
  type TDriver = MoransDriver
  // FUCKING TYPE ERASURE
  def create_param_object(ap: ArgParser) = new TParam(ap)
  def create_driver = new TDriver
}
