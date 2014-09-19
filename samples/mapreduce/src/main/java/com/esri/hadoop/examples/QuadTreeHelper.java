package com.esri.hadoop.examples;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.esri.core.geometry.Envelope2D;
import com.esri.core.geometry.Geometry;
import com.esri.core.geometry.OperatorSimpleRelation;
import com.esri.core.geometry.QuadTree;
import com.esri.core.geometry.QuadTree.QuadTreeIterator;
import com.esri.core.geometry.SpatialReference;

/**
 * Helper class that simplifies working with the in-memory quadtree.  
 * This class is <b>not thread safe</b> and has optimizations that 
 * will break with concurrent access. 
 * 
 * @param <T_Item> Type of item associated with geometry and returned on query.
 */
public class QuadTreeHelper<T_Item> {

	private final List<Geometry> geometries;
	private final List<T_Item> items;

	private final QuadTree index;
	private final RelatedElementIterator iter;
	
	private SpatialReference sref = null;
	private double tolerance = 0;

	private List<T_Item> cachedList = new ArrayList<T_Item>();
	private List<T_Item> cachedListImmutable = Collections.unmodifiableList(cachedList);
	
	/**
	 * Constructs an in memory quadtree.
	 * 
	 * @param xmin
	 * @param ymin
	 * @param xmax
	 * @param ymax
	 * @param height quadtree height (8 is a good default)
	 */
	public QuadTreeHelper(double xmin, double ymin, double xmax, double ymax, int height) {
		this(new Envelope2D(xmin, ymin, xmax, ymax), height);
	}
	
	/**
	 * Constructs an in memory quadtree.
	 * 
	 * @param extent
	 * @param height quadtree height (8 is a good default)
	 */
	public QuadTreeHelper(Envelope2D extent, int height) {
		index = new QuadTree(extent, height);
		iter = new RelatedElementIterator(index.getIterator());
		geometries = new ArrayList<Geometry>();
		items = new ArrayList<T_Item>();
	}
	
	/**
	 * Sets the spatial reference.  This is used for tolerances in
	 * relationship tests. 
	 * 
	 * @param sref spatial reference
	 */
	public void setSpatialReference(SpatialReference sref) {
		this.sref = sref;
		this.tolerance = sref == null ? 0 : sref.getTolerance();
	}
	
	/**
	 * Inserts a geometry into the quadtree.
	 * 
	 * @param geometry geometry to insert
	 * @param item associated with the geometry on query
	 * @return true, if the geometry was inserted
	 */
	public boolean insert(Geometry geometry, T_Item item) {
		Envelope2D bbox = new Envelope2D();
		geometry.queryEnvelope2D(bbox);
		
		return insert(geometry, bbox, item);
	}
	
	// insert with bounding box pre-computed
	private boolean insert(Geometry geometry, Envelope2D bbox, T_Item item) {
		if (index.insert(geometries.size(), bbox) < 0) {
			return false;
		}
		
		geometries.add(geometry);
		items.add(item);
		return true;
	}
	
	/**
	 * Returns all items for which the corresponding geometry satisfies the
	 * <code>relation</code> to <code>target</code> with the operator provided.
	 * 
	 * @param target left side of relationship test
	 * @param relation simple relationship test operator
	 * @param inverse invert relationship test (not disjoint, not contains, ...)
	 * @return an immutable list of geometries (the underlying list will change on 
	 * 		   subsequent calls to query)
	 */
	public List<T_Item> query(Geometry target, OperatorSimpleRelation relation, boolean inverse) {
		iter.reset(target, relation, inverse);
		cachedList.clear();
		
		int element = 0;
		while ((element = iter.next()) >= 0) {
			cachedList.add(items.get(element));
		}
		
		return cachedListImmutable;
	}
	
	/**
	 * Returns the first item for which the corresponding geometry satisfies the
	 * <code>relation</code> to <code>target</code> with the operator provided. 
	 * 
	 * <p>
	 * An example use case is a quadtree built with a set of completely disjoint
	 * polygons (like counties) and you are querying with points.
	 * </p>
	 * 
	 * @param target left side of relationship test
	 * @param relation simple relationship test operator
	 * @return first item that passes the relationship test, or null if none pass
	 */
	public T_Item queryFirst(Geometry target, OperatorSimpleRelation relation, boolean inverse) {
		iter.reset(target, relation, inverse);
		int element = iter.next();
		return element >= 0 ? items.get(element) : null;
	}
	
	/**
	 * Returns true if any geometry in the tree satisfies the <code>relation</code>
	 * with the target geometry
	 * 
	 * @param target left side of relationship test
	 * @param relation simple relationship test operator
	 * @param inverse invert relationship test (not disjoint, not contains, ...)
	 * @return true, if the target relates to any geometry in the tree
	 */
	public boolean hit(Geometry target, OperatorSimpleRelation relation, boolean inverse) {
		iter.reset(target, relation, inverse);
		return iter.next() >= 0;
	}
	
	private class RelatedElementIterator {
		private final QuadTreeIterator iter;
		private OperatorSimpleRelation relation;
		private Geometry target;
		private boolean inverse;
		
		public RelatedElementIterator(QuadTreeIterator iter) {
			this.iter = iter;
		}
		
		public void reset(Geometry target, OperatorSimpleRelation relation, boolean inverse) {
			iter.resetIterator(target, tolerance);
			this.relation = relation;
			this.target = target;
			this.inverse = inverse;
		}
		
		public int next() {
			// iterate until we find a geometry that passes the relation 
			// test, or until the end of the quadtree iterator
			int handle;
			while ((handle = iter.next()) >= 0) {
				int element = index.getElement(handle);
				Geometry candidate = geometries.get(element);
				
				if (relation.execute(candidate, target, sref, null)) {
					if (!inverse) {
						return element;
					}
				} else if (inverse) {
					return element;
				}
			}

			return -1;
		}
	}
}
