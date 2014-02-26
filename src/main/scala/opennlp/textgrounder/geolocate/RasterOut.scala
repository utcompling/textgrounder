package opennlp.textgrounder
package geolocate

import util.argparser._
import util.experiment._
import langmodel.Unigram
import util.textdb.TextDB

import scala.collection.mutable.Map

import java.io._

class RasterOutParameters(
  parser: ArgParser
) extends GeolocateParameters(parser) {

//File to write ascii raster to
  var raster_output =
    ap.option[String]("ro", "raster_out",
      metavar = "SRTING",
      default = "none",
      help = """Name of raster out file""")
//Word to create raster for
  var word_raster =
      ap.option[String]("word_raster", "wordraster",
      metavar = "STRING",
      default = "",
      help = """Name of the word who's probabilities will be output""")
}

class RasterDriver extends
    GeolocateDriver with StandaloneExperimentDriverStats {
  type TParam = RasterOutParameters
  type TRunRes = Unit

  println(params)

  /**
   * Generate the salience grid.
   */

  // def CalcMoransWord(word:String, wordFreqsRef:File, GridReference:File){
  //
  // }

  def run() {
    val xgrid = initialize_grid
    val grid = xgrid match {
      case g: MultiRegularGrid => g
      case _ => throw new IllegalArgumentException("RasterOut needs a regular grid, not a K-d grid")
    }

    println("###### Starting Raster ######")
    println(params.word_raster)
    //println(RasterOutParameters.word_raster)
    //println(RasterOutParameters.raster_output)
    //First Create Grid
    println(params.raster_output)
    println(params.word_raster)
    println(params.degrees_per_cell)
    val cellSize = params.degrees_per_cell
    val raster_output = params.raster_output
    val word_raster = params.word_raster
    //val word_raster = "mama"
    //val raster_output = "/Users/grant/devel/tg-experiments/RasterOutTest.txt"

    val wordMoransMap = Map[Int, Double]()
    val cellsInfo = Map[String, List[String]]()

    //println(grid.getClass.getSimpleName)
    val wordMap_lookupMap = getWordMeans(grid)

    val word = word_raster
    val wordid = wordMap_lookupMap._2(word)
    val degsize = (params.degrees_per_cell).toString()

    for (xcell <- grid.iter_nonempty_cells) {
      val cell = xcell.asInstanceOf[MultiRegularCell]

      val w_coord = cell.get_southwest_coord.long
      val s_coord = cell.get_southwest_coord.lat

      val index = cell.index

      val CP = cellProb(cell.index.latind, cell.index.longind, grid, wordid)
      cellsInfo += ((index.latind + "-" + index.longind) -> List(CP.toString(), w_coord.toString(), s_coord.toString()))

      val lm = Unigram.check_unigram_lang_model(cell.lang_model.grid_lm)

    }

    val dir = new File(raster_output)
    val fw = new FileWriter(dir.getAbsoluteFile())
    println(dir.getAbsoluteFile())
    val bw = new BufferedWriter(fw)

    //import scala.collection.mutable.List

    var rasterList = List[List[Any]]()

    val rasterList2 = cellsInfo.map(createRasterList(_, word, degsize.toDouble))
    // Next four are equivalent:
    // 1: Sort by, less abbreviated
    //rasterList2.toSeq.sortBy(x => x._3).sortBy(x => -x._2)
    // 2: Sort by, more abbreviated
    //rasterList2.toSeq.sortBy(_._3).sortBy(-_._2)
    // 3: Sort with, less abbreviated
    //rasterList2.toSeq.sortWith((x,y) => x._3 < y._3).sortWith((x,y) => x._2 > y._2)
    // Python equivalent:
    // rasterList2.sort(lambda x,y: x(3) < y(3)).sort(lambda x,y: x(2) > y(2))
    // 4: Sort with, more abbreviated
    val rasterList3 = rasterList2.toSeq.sortWith(_._5 > _._5)

    val rasterList4 = rasterList2.toSeq.sortWith(_._6 < _._6)

    //smallest lat ID
    //println(rasterList2.toSeq.sortWith(_._5 < _._5))
    val leastLatId = rasterList2.toSeq.sortWith(_._5 < _._5)(0)._3
    val yllCorner = rasterList2.toSeq.sortWith(_._5 < _._5)(0)._5
    //println(leastLatId)

    //largest Lat ID
    val largestLatId = rasterList3(0)._3
    //println(rasterList3)
    val largestLat = rasterList3(0)._5
    //println(largestLatId)

    //least Long ID
    val leastLongId = rasterList4(0)._4
    //println(rasterList4)
    val leastLong = rasterList4(0)._6
    val xllCorner = rasterList4(0)._6
    //println(leastLongId)

    //largest Long ID
    val largestLongId = rasterList2.toSeq.sortWith(_._6 > _._6)(0)._4
    println(largestLongId)

    //Raster Header Info
    val nrows = scala.math.abs(largestLongId - leastLongId)
    val ncols = (largestLatId - leastLatId)
    println("####Raster Header####")
    println(nrows)
    println(ncols)
    println(xllCorner)
    println(yllCorner)
    println(cellSize)
    val no_data = -99
    println(no_data)

    bw.write("NCOLS " + nrows + "\n")
    bw.write("NROWS " + ncols + "\n")
    bw.write("XLLCORNER " + xllCorner + "\n")
    bw.write("YLLCORNER " + yllCorner + "\n")
    bw.write("CELLSIZE " + cellSize + "\n")
    bw.write("nodata_value " + no_data + "\n")

    /*println("####Least and Most Long ID###")
    println(leastLongId)
    println(largestLongId)
    println("###Least and Most Lat ID###")
    println(leastLatId)
    println(largestLatId)
    println("###Largest Latitude###")
    println(largestLat)
    println("###Least Long###")
    println(leastLong)*/

    var i = leastLongId
    var j = largestLatId
    while(j > leastLatId ){
      i = leastLongId
      while(i > largestLongId){
        //println(i)
        //println(i+"-"+j)
        //println(cellsInfo.contains((i+"-"+j))
        val matcher = j + "--" + i
        if (cellsInfo.contains(matcher)){
          //println("Found one:: " + j+"--"+i)
        }
        if (cellsInfo.contains(j+"--"+i)){
          //println(cellsInfo(j+"--"+i)(2) + "--" + cellsInfo(j+"--"+i)(1))
          bw.write(cellsInfo(j+"--"+i)(0) + " ")
          }else{
            bw.write("-99  ")
          }
        i = i - 1
      }
      j = j - 1
      bw.write("\n")
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

  def createRasterList(cellsInfo:(String, List[String]), word:String, degsize:Double) = {
  	val lat_ind = cellsInfo._1.split("--")(0)
  	val long_ind = cellsInfo._1.split("--")(1)
  	val nw_latcoord = cellsInfo._2(2)
  	val nw_longcoord = cellsInfo._2(1)
  	val word_prob = cellsInfo._2(0)
  	val listLine  = (word, degsize, lat_ind.toInt, long_ind.toInt, nw_latcoord.toDouble, nw_longcoord.toDouble, word_prob)
  	listLine
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
    val idToWordMap = Map[String, Int]()
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
          idToWordMap += (lm.gram_to_string(word) -> word)
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

object RasterOutApp extends GeolocateApp("RasterOut") {
  type TDriver = RasterDriver
  // FUCKING TYPE ERASURE
  def create_param_object(ap: ArgParser) = new TParam(ap)
  def create_driver = new TDriver
}
