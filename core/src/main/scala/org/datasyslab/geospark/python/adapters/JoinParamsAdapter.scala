package org.datasyslab.geospark.python.adapters


import org.datasyslab.geospark.enums.{IndexType, JoinBuildSide}
import org.datasyslab.geospark.spatialOperator.JoinQuery.JoinParams
import com.vividsolutions.jts.geom.Geometry

object JoinParamsAdapter {
  def createJoinParams(useIndex: Boolean = false, indexType: String, joinBuildSide: String): JoinParams[Geometry,Geometry] = {
    val buildSide = JoinBuildSide.getBuildSide(joinBuildSide)
    val currIndexType = IndexType.getIndexType(indexType)
    new JoinParams(useIndex, currIndexType, buildSide)
  }
}