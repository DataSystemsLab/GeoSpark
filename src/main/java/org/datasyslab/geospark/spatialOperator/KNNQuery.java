package org.datasyslab.geospark.spatialOperator;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.datasyslab.geospark.spatialRDD.PointRDD;

import com.vividsolutions.jts.geom.Point;


public class KNNQuery {
	/**
	 * Spatial K Nearest Neighbors query
	 * @param pointRDD specify the input pointRDD
	 * @param p specify the query center 
	 * @param k specify the K
	 * @return A list which contains K nearest points
	 */
	public static List<Point> SpatialKnnQuery(PointRDD pointRDD, final Broadcast<Point> p, final Integer k) {
		// For each partation, build a priority queue that holds the topk
		@SuppressWarnings("serial")
		class PointCmp implements Comparator<Point>, Serializable {

			public int compare(Point p1, Point p2) {
				double distance1 = p1.getCoordinate().distance(p.value().getCoordinate());
				double distance2 = p2.getCoordinate().distance(p.value().getCoordinate());
				if (distance1 > distance2) {
					return 1;
				} else if (distance1 == distance2) {
					return 0;
				}
				return -1;
			}
		}
		final PointCmp pcmp = new PointCmp();

		JavaRDD<Point> tmp = pointRDD.getRawPointRDD().mapPartitions(new FlatMapFunction<Iterator<Point>, Point>() {

			public Iterable<Point> call(Iterator<Point> input) throws Exception {
				PriorityQueue<Point> pq = new PriorityQueue<Point>(k, pcmp);
				while (input.hasNext()) {
					if (pq.size() < k) {
						pq.offer(input.next());
					} else {
						Point curpoint = input.next();
						double distance = curpoint.getCoordinate().distance(p.getValue().getCoordinate());
						double largestDistanceInPriQueue = pq.peek().getCoordinate()
								.distance(p.value().getCoordinate());
						if (largestDistanceInPriQueue > distance) {
							pq.poll();
							pq.offer(curpoint);
						}
					}
				}

				ArrayList<Point> res = new ArrayList<Point>();
				for (int i = 0; i < k; i++) {
					res.add(pq.poll());
				}
				return res;
			}
		});

		// Take the top k

		return tmp.takeOrdered(k, pcmp);

	}
}
