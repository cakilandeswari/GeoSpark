
package com.vzmedia.location.joinJudgement;

import com.vividsolutions.jts.geom.*;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.FlatMapFunction2;
import org.datasyslab.geospark.joinJudgement.DedupParams;
import org.datasyslab.geospark.joinJudgement.JudgementBase;
import org.datasyslab.geospark.utils.GeodesicArea;
import org.geotools.geometry.jts.JTS;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 *  This class computes spatial joins based on given percentage of area overlap
 *  and returns meta data in the list of places within and containing.
 */
public class NestedLoopSpatialOverlapJudgement<T extends Geometry, U extends Geometry>
    extends JudgementBase
    implements FlatMapFunction2<Iterator<T>, Iterator<U>, Pair<U, T>>, Serializable
{

    private enum REL { CONTAINS, WITHIN, NONE};
    private static final Logger log = LogManager.getLogger(NestedLoopSpatialOverlapJudgement.class);
    private final double areaOverlap;

    /**
     * @see JudgementBase
     */
    public NestedLoopSpatialOverlapJudgement(double areaOverlap, @Nullable DedupParams dedupParams)
    {
        super(true, dedupParams);
        this.areaOverlap = areaOverlap;
    }

    @Override
    public Iterator<Pair<U, T>> call(Iterator<T> iteratorObject, Iterator<U> iteratorWindow)
    {
        initPartition();

        List<Pair<U, T>> result = new ArrayList<>();
        List<T> queryObjects = new ArrayList<>();
        while (iteratorObject.hasNext()) {
            queryObjects.add(iteratorObject.next());
        }
        StringBuffer contains = new StringBuffer();
        StringBuffer within = new StringBuffer();
        boolean found;
        while (iteratorWindow.hasNext()) {
            U window = iteratorWindow.next();
            found=false;
            contains.setLength(0);
            within.setLength(0);
            contains.append("0");
            within.append("0");
            for (int i = 0; i < queryObjects.size(); i++) {
                T object = queryObjects.get(i);
                REL relation =applyJoinTransform(window,object);
                if (relation==REL.CONTAINS) {
                    contains.append(",");
                    contains.append(object.getUserData());
                    found=true;
                }
                if (relation==REL.WITHIN) {
                    within.append(",");
                    within.append(object.getUserData());
                    found=true;
                }
            }
            if (found) {
                Geometry temp1 =  window.getCentroid();
                Geometry temp2 =  window.getCentroid();
                temp1.setUserData(window.getUserData());
                temp2.setUserData(contains.toString() + "\t" + within.toString());
                result.add(Pair.of((U) temp1 , (T) temp2 ));
            }
        }
        return result.iterator();
    }

    private REL applyJoinTransform(Geometry leftGeometry, Geometry rightGeometry){

        // if both are points, don't bother
        if (leftGeometry instanceof Point && rightGeometry instanceof Point)
            return REL.NONE;

        double intArea;

        try {
            IntersectionMatrix intersectionMatrix;
            try {
                intersectionMatrix = leftGeometry.relate(rightGeometry);
            } catch (Exception ex) {
                leftGeometry       =  leftGeometry.buffer(0);
                rightGeometry      =  rightGeometry.buffer(0);
                intersectionMatrix = leftGeometry.relate(rightGeometry);
            }

            if (intersectionMatrix.isDisjoint()) {
                return REL.NONE;
            }

            if (intersectionMatrix.isContains() || intersectionMatrix.isCovers()) {
                return REL.CONTAINS;
            }

            if (intersectionMatrix.isWithin() || intersectionMatrix.isCoveredBy()) {
                return REL.WITHIN;
            }

            try {
                intArea = GeodesicArea.computeArea(leftGeometry.intersection(rightGeometry));
            } catch (Exception ex) {
                leftGeometry       =  leftGeometry.buffer(0);
                rightGeometry      =  rightGeometry.buffer(0);
                intArea            = GeodesicArea.computeArea(leftGeometry.intersection(rightGeometry));
            }


            double leftArea        =  GeodesicArea.computeArea(leftGeometry);
            double rightArea       =  GeodesicArea.computeArea(rightGeometry);

            if (intArea/rightArea >= this.areaOverlap) {
                return REL.CONTAINS;
            }
            if (intArea/leftArea >= this.areaOverlap) {
                return REL.WITHIN;
            }
        } catch (Exception ex) {
            System.out.println(leftGeometry.getUserData().toString().trim() + "|" + rightGeometry.getUserData().toString().trim() + "|" + ex.getMessage());
            ex.printStackTrace();
        }

        return REL.NONE;
    }
}