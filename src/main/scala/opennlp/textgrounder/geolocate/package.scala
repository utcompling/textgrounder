package opennlp.textgrounder

import gridlocate._
import util.distances._

package object geolocate {
  /**
   * A general trait holding SphereDocument-specific code for storing the
   * result of evaluation on a document.  Here we simply compute the
   * true and predicted "degree distances" -- i.e. measured in degrees,
   * rather than in actual distance along a great circle.
   */
  class SphereDocumentEvaluationResult(
    stats: DocumentEvaluationResult[SphereCoord, SphereDocument]
  ) {
    /**
     * Distance in degrees between document's coordinate and central
     * point of true cell
     */
    val true_degdist =
      stats.document.degree_distance_to_coord(stats.true_center)
    /**
     * Distance in degrees between doc's coordinate and predicted
     * coordinate
     */
    val pred_degdist =
      stats.document.degree_distance_to_coord(stats.pred_coord)
  }

  implicit def to_SphereDocumentEvaluationResult(
    stats: DocumentEvaluationResult[SphereCoord, SphereDocument]
  ) = new SphereDocumentEvaluationResult(stats)

  class SphereDocumentFuns(doc: DistDocument[SphereCoord]) {
    def degree_distance_to_coord(coord2: SphereCoord) =
      degree_dist(doc.coord, coord2)
  }

  implicit def to_SphereDocumentFuns(
    docs: DistDocument[SphereCoord]
  ) = new SphereDocumentFuns(docs)

  type SphereDocument = DistDocument[SphereCoord]
  type SphereCell = GeoCell[SphereCoord, SphereDocument]
  type SphereCellGrid = CellGrid[SphereCoord, SphereDocument]
  def get_sphere_doctable(grid: SphereCellGrid) =
    grid.table.asInstanceOf[SphereDocumentTable]
}
