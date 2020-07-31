package org.datasyslab.geospark.utils;

import com.vividsolutions.jts.geom.*;
import net.sf.geographiclib.Geodesic;
import net.sf.geographiclib.PolygonArea;

public class GeodesicArea {

    public static double computeArea(Geometry geom){
        PolygonArea polygonArea = new PolygonArea(Geodesic.WGS84, false);
        if (geom instanceof MultiPolygon) {
            MultiPolygon multipolygon = (MultiPolygon) geom;
            int polygonParts = multipolygon.getNumGeometries();
            double area = 0;
            for (int i = 0; i < polygonParts; ++i) {
                com.vividsolutions.jts.geom.Polygon polygon = (com.vividsolutions.jts.geom.Polygon) multipolygon.getGeometryN(i);
                area += computeArea(polygon);
            }
            return area;
        }
        else if (geom instanceof Polygon) {
            return computeArea(polygonArea,(Polygon) geom);
        }
        else if (geom instanceof GeometryCollection) {
            double area = 0;
            for (int i = 0; i < geom.getNumGeometries(); ++i) {
                area += computeArea(geom.getGeometryN(i));
            }
            return area;
        }
        else {
            return geom.getArea();
        }
    }
    /**
     * This function computes the area of the polygon excluding the area of the rings using
     * C. F. F. Karney, Algorithms for geodesics (2013)
     *
     * @param polygon input polygon
     * @return int area of the polygon in square meters minus area of the inner rings.
     */
    private static double computeArea(PolygonArea polygonArea, com.vividsolutions.jts.geom.Polygon polygon) {
        polygonArea.Clear();
        for (Coordinate coordinate : polygon.getExteriorRing().getCoordinates()) {
            polygonArea.AddPoint(coordinate.y, coordinate.x);
        }

        /* The function returns a signed result for the area if the polygon is traversed
         * in the clockwise direction, instead of returning the area for the rest of the earth.
         * It is SAFE to use the absolute value returned from the function call.
         */
        double outerArea = Math.abs(polygonArea.Compute().area);

        // remove the area of the rings now
        if (polygon.getNumInteriorRing() > 0) {
            for (int j = 0; j < polygon.getNumInteriorRing(); ++j) {
                polygonArea.Clear();
                for (Coordinate coordinate : polygon.getInteriorRingN(j).getCoordinates()) {
                    polygonArea.AddPoint(coordinate.y, coordinate.x);
                }
                outerArea -= Math.abs(polygonArea.Compute().area);
            }
        }

        return outerArea;
    }


}