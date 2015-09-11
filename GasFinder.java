package org.bar.geo;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Pattern;

import org.bar.geo.KdTree.EuclideanComparator;
import org.bar.geo.KdTree.KdNode;
import org.bar.geo.KdTree.XYZPoint;
import org.bar.geo.Main.GeoPoint;

public class GasFinder<T extends KdTree.XYZPoint> {
	private static final double MILES_BUFFER = 20;
	private static final double M_IN_MILES =1609.34;
	private static KdTree gasTree;
	public static HashMap stations = new HashMap(); 
	private static final String FULL_DATA_FILE = "fulldatafile.txt";
	
	public GasFinder() throws NumberFormatException, IOException{
		File file = new File(FULL_DATA_FILE);
	    BufferedReader in = new BufferedReader(new FileReader(file));
	    String line;
	    ArrayList<XYZPoint> points = new ArrayList(); 
	    // Disregard first line
	    line = in.readLine();
	    
	    // Loop through all our data
	    while ((line = in.readLine()) != null) {
	    	String[] data = line.split(Pattern.quote("|||"));
	    	XYZPoint pt = new XYZPoint(Double.parseDouble(data[0]), Double.parseDouble(data[1]), Double.parseDouble(data[2]));
	    	stations.put(pt, data);
	    	points.add(pt);
	    }
	    gasTree =  new KdTree(points);
	}
	
	 public ArrayList<GasStationNode> findCheapestStationsAlongRoute(double r, ArrayList<GeoPoint> route) {
		 TreeSet<GasStationNode> results = new TreeSet<GasStationNode>();
		 for (int i = 0; i < route.size(); i++){
			
			double[] xyz = latlngelevToXYZ(route.get(i).lat, route.get(i).lng, route.get(i).elev);
		    XYZPoint pt = new XYZPoint(xyz[0], xyz[1], xyz[2]);
		    GasStationNode gs = findCheapestStation(M_IN_MILES, pt);
		    if (gs != null ){
		    	results.add(gs);
		    	
		    }
		 }
		 
		 ArrayList<GasStationNode> collection = new ArrayList<GasStationNode>();
	        for (GasStationNode gsNode : results) {
	            collection.add(gsNode);
	        }
	        return collection;
	    }
	 
	public ArrayList<GasStationInRouteNode> planCheapestStops(double r, ArrayList<GeoPoint> route, double gallons, int mpg, double initGasTank){
		double cumDistance = 0;
		double[] xyzPrev = latlngelevToXYZ(route.get(0).lat, route.get(0).lng, route.get(0).elev);
	    XYZPoint prev = new XYZPoint(xyzPrev[0], xyzPrev[1], xyzPrev[2]);
		TreeSet<GasStationInRouteNode> cheapestStations = new TreeSet<GasStationInRouteNode>(new GasStationComparator());
		TreeSet<TripState> tripStates = new TreeSet<TripState>();
		ArrayList<GasStationInRouteNode> stationsToVisit = new ArrayList<GasStationInRouteNode>();
		
		for (int i = 0; i < route.size(); i++){
			double[] xyz = latlngelevToXYZ(route.get(i).lat, route.get(i).lng, route.get(i).elev);
		    XYZPoint pt = new XYZPoint(xyz[0], xyz[1], xyz[2]);
		    cumDistance = cumDistance + prev.euclideanDistance(pt);
		    prev = pt;
		    GasStationNode gs = findCheapestStation(M_IN_MILES, pt);
		    if (gs != null ){
		    	System.out.println(i + " " + gs.price);
		    	double additionalDistance = gs.id.euclideanDistance(pt);
		    	double normalDistance = 0;
		    	if (i < route.size()-1){
			    		double[] xyzNext = latlngelevToXYZ(route.get(i+1).lat, route.get(i+1).lng, route.get(i+1).elev);
			    		XYZPoint next = new XYZPoint(xyzNext[0], xyzNext[1], xyzNext[2]);
			    		additionalDistance = additionalDistance + gs.id.euclideanDistance(next);
			    		normalDistance = pt.euclideanDistance(next);
		    	} else {
		    		additionalDistance = additionalDistance + gs.id.euclideanDistance(pt);
		    	}
		    	System.out.println("additionalDistance = " + additionalDistance + " normal Distance: " + normalDistance);
		    	GasStationInRouteNode gsir = new GasStationInRouteNode(gs,cumDistance, additionalDistance - normalDistance, gs.id.euclideanDistance(pt), i);
		    	cheapestStations.add(gsir);
		   }
		}
		
		HashMap<Integer,GasStationInRouteNode> cheapStationMap = new HashMap<Integer,GasStationInRouteNode>();
		for (GasStationInRouteNode gsNode : cheapestStations) {
		    cheapStationMap.put(gsNode.waypointIndex, gsNode);
		}
	
		double totalDistance = cumDistance;
		double maxMiles = gallons*mpg*initGasTank;
		HashSet<XYZPoint> visitedGas = new HashSet(); 
		boolean[] visitedWayPoint  = new boolean[cheapestStations.size()];
		
		
		
		// Find the first cheapest station within the car's mileage range and gas up there!
		for (GasStationInRouteNode gsNode : cheapestStations) {
			// Check if gas station is within the car's maximum mileage
			if (maxMiles < gsNode.cumulativeDistance + MILES_BUFFER) continue;
			
			// Check if current gas station is already investigated
			if (visitedGas.contains(gsNode.id)) continue;
			visitedGas.add(gsNode.id);
			
			
			
			// Gas up there
			double distanceToGasStation = gsNode.cumulativeDistance + gsNode.distanceFromLastWaypoint;
			double moneySpent = (distanceToGasStation*gsNode.price)/mpg;
			double distanceLeft = totalDistance + gsNode.additionalDistance - distanceToGasStation;
			double gasTankLevel = 1;
			
			TripState state = new TripState(moneySpent, distanceLeft, gasTankLevel, gsNode.waypointIndex);
			stationsToVisit.add(gsNode);
			tripStates.add(state);
			break;
		}
		
		while (tripStates.size() > 0){
			TripState currState = tripStates.first();
			tripStates.remove(tripStates.first());
			
			// Check if current waypoint is already investigated
			if (visitedWayPoint[currState.currRouteIndex]) continue;
			visitedWayPoint[currState.currRouteIndex] = true;
			
			// Check if we are in our destination
			if (currState.currRouteIndex == route.size()-1){
				//Attempt to fill the tank there
				if (cheapStationMap.containsKey(currState.currRouteIndex)){
					GasStationInRouteNode gsNode = cheapStationMap.get(currState.currRouteIndex);
					// Check if we have visited this gas station already
					if (!visitedGas.contains(gsNode.id)){
						double distanceToGasStation = gsNode.distanceFromLastWaypoint;
						double moneySpent = currState.moneySpent + (distanceToGasStation*gsNode.price)/mpg + ((maxMiles*(1-currState.gasTankLevel))*gsNode.price)/mpg;
						double distanceLeft = distanceToGasStation;
						double gasTankLevel = 1;
						TripState state = new TripState(moneySpent, distanceLeft, gasTankLevel, gsNode.waypointIndex);
						System.out.println("Total Money Spent:" + moneySpent);
						stationsToVisit.add(gsNode);
					}
				}
			}
			
			// Check to see if gassing up in the next station is the cheapest option
			if (cheapStationMap.containsKey(currState.currRouteIndex + 1)){
				GasStationInRouteNode gsNode = cheapStationMap.get(currState.currRouteIndex + 1);
				double distanceToGasStation = gsNode.distanceFromLastWaypoint + currState.milesToGo - (totalDistance - gsNode.cumulativeDistance);
				double moneySpent = currState.moneySpent + (distanceToGasStation*gsNode.price)/mpg + ((maxMiles*(1-currState.gasTankLevel))*gsNode.price)/mpg;
				double distanceLeft = currState.milesToGo - distanceToGasStation + gsNode.additionalDistance - gsNode.distanceFromLastWaypoint;
				double gasTankLevel = 1;
				TripState stateGasUpInNext = new TripState(moneySpent, distanceLeft, gasTankLevel, gsNode.waypointIndex);
				tripStates.add(stateGasUpInNext);
			}
			
			// Check to see if not gassing up in the next station is the cheapest option
			//double moneySpent = currState.moneySpent + (distanceToGasStation*gsNode.price)/mpg + ((maxMiles*(1-currState.gasTankLevel))*gsNode.price)/mpg;
			double gasTankLevel = 1;
			//TripState stateGasUpInNext = new TripState(moneySpent, distanceLeft, gasTankLevel, gsNode.waypointIndex);
			//tripStates.add(stateGasUpInNext);
			
		}
		
		return stationsToVisit;
		
	}
	
	public static class TripState implements Comparable<TripState>{
		double moneySpent;
		double milesToGo;
		double gasTankLevel;
		int currRouteIndex;
		
		public TripState (double moneySpent, double milesToGo, double gasTankLevel, int currRouteIndex){
			this.moneySpent = moneySpent;
			this.milesToGo = milesToGo;
			this.gasTankLevel = gasTankLevel;
			this.currRouteIndex = currRouteIndex;
		}

		public int compareTo(TripState o) {
			if (moneySpent < o.moneySpent) return -1;
			if (moneySpent > o.moneySpent) return 1;
			if (milesToGo > o.milesToGo) return -1;
			if (milesToGo < o.milesToGo) return 1;
			if (gasTankLevel > o.gasTankLevel) return -1;
			if (gasTankLevel < o.gasTankLevel) return 1;
			return 0;
		}
		
		
	}
	
	public GasStationNode findCheapestStation(double r, XYZPoint value ) {
        if (value == null)
            return null;
        KdNode node = gasTree.root();
        // Create map used for results
        TreeSet<GasStationNode> results = new TreeSet<GasStationNode>();
        // Create hashset used to store examined nodes
        Set<KdNode> examined = new HashSet<KdNode>();
        
        // Go down the tree starting from root to find neighbors
        findNeighbors(value, node, r, results, examined);
        
        try {
        return results.first();
        } catch (Exception e){
        	return null;
        }
	}
	
	/**
     * Find all stations within r radius of an XYZ point
     * @param r
     *            Search radius. Only picks up stations within this radius.
     * @param value
     *            XYZ point to find neighbors of.
     *            
     * @return collection of T neighbors.
     */
    @SuppressWarnings("unchecked")
    public Collection<Double> stationsWithinFixedRadius(double r, T value) {
        if (value == null)
            return null;
        
        KdNode node = gasTree.root();
        // Create map used for results
        TreeSet<GasStationNode> results = new TreeSet<GasStationNode>();
        // Create hashset used to store examined nodes
        Set<KdNode> examined = new HashSet<KdNode>();
        
        // Go down the tree starting from root to find neighbors
        findNeighbors(value, node, r, results, examined);
        
        // Load up the collection of the results
        Collection<Double> collection = new ArrayList<Double>();
        for (GasStationNode gsNode : results) {
            collection.add(gsNode.price);
        }
        return collection;
    }
    
   private static final <T extends KdTree.XYZPoint> void findNeighbors(T value, KdNode node, double r, 
    		TreeSet<GasStationNode> results, Set<KdNode> examined) {
        // Add node to examined tree
    	examined.add(node);
    	// Find distance between node and test point
    	Double nodeDistance = node.getXYZPoint().euclideanDistance(value);
    	
    	// Check if distance is less than or equal to r, and if it is add it to our results
    	if (nodeDistance.compareTo(r) <= 0){
    		double price;
    		try {
    			price = Double.parseDouble(((String[]) stations.get(node.getXYZPoint()))[6]);
    			GasStationNode gsNode = new GasStationNode(price, node.getXYZPoint());
        		results.add(gsNode);
    		} catch (NumberFormatException e){
    		}
    	}
    	
    	// Find the axis of our current mode and create pointers to the current node's lesser and greater subtree root nodes 
    	int axis = node.getDepth() % node.getDimension();
        KdNode lesser = node.lesser;
        KdNode greater = node.greater;

        // Find distance between the current hyperplane axis and our test point
        double absAxisToPointDistance = Double.MIN_VALUE;
        double axisToPointDistance = Double.MIN_VALUE;
        
        if (axis == KdTree.X_AXIS) {
        	absAxisToPointDistance = Math.abs(node.getXYZPoint().getX() - value.getX());
        	axisToPointDistance = node.getXYZPoint().getX() - value.getX();
        	
        } else if (axis == KdTree.Y_AXIS) {
        	absAxisToPointDistance = Math.abs(node.getXYZPoint().getY() - value.getY());
        	axisToPointDistance = node.getXYZPoint().getY() - value.getY();
        } else {
        	absAxisToPointDistance = Math.abs(node.getXYZPoint().getZ() - value.getZ());
        	axisToPointDistance = node.getXYZPoint().getZ() - value.getZ();
        }
        
        // If axis to test point distance is greater than r, then hyperplane axis intersects our search sphere 
        boolean lineIntersectsSphere = ((absAxisToPointDistance <= r) ? true : false);
        
        // If axis to test point distance is negative, then that means our test point is in the greater hyperspace
        boolean pointInGreaterPlane = ((axisToPointDistance <= 0)? true : false);
        
        // If our point is on the greater hyperspace, search for potential neighbors in the greater hyperspace
        if (pointInGreaterPlane){
        	if (greater != null && !examined.contains(greater)) {
				 examined.add(greater);
			     findNeighbors(value, greater, r, results, examined);
			}
			// If our search sphere intersects the hyperplane, search for neighbors in the lesser hyperspace 
		    if (lineIntersectsSphere){
		    	 if (lesser != null && !examined.contains(lesser)) {
		    		 examined.add(lesser);
		    		 findNeighbors(value, lesser, r, results, examined);
		    	 }
		    }
        } 
        // If our point is on the lesser hyperspace, search for potential neighbors in the lesser hyperspace
        else {
        	if (lesser != null && !examined.contains(lesser)) {
				examined.add(lesser);
				findNeighbors(value, lesser, r, results, examined);
			}
        	// If our search sphere intersects the hyperplane, search for neighbors in the greater hyperspace 
			if (lineIntersectsSphere){
				if (greater != null && !examined.contains(greater)) {
					 examined.add(greater);
				     findNeighbors(value, greater, r, results, examined);
		    	 }
		    }
        	
        }
    }
    
    public static class GasStationNode implements Comparable<GasStationNode> {
    	double price;
    	XYZPoint id;
    	public GasStationNode(double price, XYZPoint id){
    		this.price = price;
    		this.id = id;
    	}
    	
    	public int compareTo(GasStationNode o) {
    		 if (price < o.price) return -1;
    		 if (price > o.price) return 1;
    		 return id.compareTo(o.id);
        }
    }
    
    public static class GasStationComparator implements Comparator<GasStationInRouteNode>{
    	public int compare(GasStationInRouteNode o1, GasStationInRouteNode o2) {
    		int output = o1.compareTo(o2);
    		if (output == 0){
    			if (o1.additionalDistance < o2.additionalDistance) return -1;
				if (o1.additionalDistance > o2.additionalDistance) return 1;
	    		if (o1.waypointIndex < o2.waypointIndex) return -1;
				if (o1.waypointIndex > o2.waypointIndex) return 1;
				return 0;
    		} else {
    			return output;
    		}
	   		
    	}
    }
   
    public static class GasStationInRouteNode extends GasStationNode{
    	double cumulativeDistance;
    	double distanceFromLastWaypoint;
    	double additionalDistance;
    	int waypointIndex;
    	
    	public GasStationInRouteNode(double price, XYZPoint id, double cumulativeDistance, double additionalDistance, double distanceFromLastWaypoint, int waypointIndex){
    		super(price, id);
    		this.cumulativeDistance = cumulativeDistance;
    		this.additionalDistance = additionalDistance;
    		this.distanceFromLastWaypoint = distanceFromLastWaypoint;
    		this.waypointIndex = waypointIndex;
    	}
    	
    	public GasStationInRouteNode(GasStationNode node, double cumulativeDistance, double additionalDistance, double distanceFromLastWaypoint, int waypointIndex){
    		super(node.price, node.id);
    		this.cumulativeDistance = cumulativeDistance;
    		this.additionalDistance = additionalDistance;
    		this.distanceFromLastWaypoint = distanceFromLastWaypoint;
    		this.waypointIndex = waypointIndex;
    	}
    	
    	public String toString() {
    		return "price: " + price + " waypointIndex: " + waypointIndex + " cumulativeDistance: " + cumulativeDistance + " additionalDistance: " + additionalDistance;
    	}
    	
    }
    
    public static double[] latlngelevToXYZ(double lat, double lng, double elev){
		double cosLat = Math.cos(lat * Math.PI / 180.0);
		double sinLat = Math.sin(lat * Math.PI / 180.0);
		double cosLon = Math.cos(lng * Math.PI / 180.0);
		double sinLon = Math.sin(lng * Math.PI / 180.0);
		double rad =  6371000;
		double f = 1.0 / 298.257224;
		double C = 1.0 / Math.sqrt(cosLat * cosLat + (1 - f) * (1 - f) * sinLat * sinLat);
		double S = (1.0 - f) * (1.0 - f) * C;
		double h = elev;
		
		double output[] = new double[3];
		output[0] = (rad * C + h) * cosLat * cosLon;
		output[1] = (rad * C + h) * cosLat * sinLon;
		output[2] = (rad * S + h) * sinLat;
		
		return output;
	}
    

}
