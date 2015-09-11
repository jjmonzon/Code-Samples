package org.bar.geo;


import java.io.*;
import java.lang.*;
import java.net.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Pattern;

import javax.net.ssl.HttpsURLConnection;

import org.bar.geo.GasFinder.GasStationInRouteNode;
import org.bar.geo.GasFinder.GasStationNode;
import org.bar.geo.KdTree.KdNode;
import org.bar.geo.KdTree.XYZPoint;
import org.json.simple.*;
import org.json.simple.parser.*;

/**
 * Class that determines the list of cheapest gas stations along a route
 * between two locations. Leverages both google api and myGasFeed api
 * 
 * 
 * @author Josh Monzon
 * 
 */
public class Main {
	private static final int maxLoc = 80;
	private static final String STATION_FILE_MASTER = "gasStationList-master.txt";
	private static final String LATLNGELEV_FILE_MASTER = "latlngelev-master.txt";
	private static final String COORDS_FILE = "coordsForElevation.txt";
	private static final String LATLNGELEV_FILE = "latlngelev.txt";
	private static final String FULL_DATA_FILE = "fulldatafile.txt";
	private static final double LNG_TEN_MILE_OFFSET =  .1579213;
	private static final double LAT_TEN_MILE_OFFSET =  .1438913;
	private static final double MIN_US_LAT = 24.396308;
	private static final double MAX_US_LAT = 49.384358;
	private static final double MAX_US_LNG = -66.885444;
	private static final double MIN_US_LNG = -124.848974;
	private static final double M_IN_MILES =1609.34;
	private static HashMap allStations = new HashMap();
	private static KdTree stationKdTree;
	private final String USER_AGENT = "Mozilla/5.0";
	private static final String APIKEY = "rfej9napna";
	private static final String DIRECTIONS_API_KEY="AIzaSyBdfXZY8quLQAv6NVu2z4bJTpYxHvLe8Io";
	private enum GasType {
		reg,mid,pre,diesel 
	}
	
	private enum SortBy{
		distance,price
	}
	
	private enum Units {
		km,miles 
	}
	
	private static PrintWriter writer; 
	private static PrintWriter logwriter; 

	public static void main(String[] args) throws Exception {
		GasFinder gsf = new GasFinder();
    	String origin = googleAPIFormat("24 Pinckney St,Somerville,MA");
    	String dest = googleAPIFormat("60 Loon Mountain Road,Lincoln,NH");
    	ArrayList<GeoPoint> routes = getRoute(origin, dest);
    	ArrayList<GeoPoint> routeswithelev = getRouteElevation(routes);
    	
    	ArrayList<GasStationInRouteNode> a = (ArrayList<GasStationInRouteNode>) gsf.planCheapestStops(M_IN_MILES, routeswithelev, 17.5 , 22, 1);
    	System.out.println(a.size());
    	for (int i = 0; i < a.size(); i++){
    		System.out.println(a.get(i).toString());
    	}
		
    	
	}
	
	public static ArrayList<GeoPoint> getRouteElevation(ArrayList<GeoPoint> routes) throws Exception{
		ArrayList<GeoPoint> output = new ArrayList<GeoPoint>();
		String coordsLine = "";
		int ctr = 0;
	    for (int i = 0; i < routes.size(); i++) {
	    	if (ctr == maxLoc){
	    		ctr = 0;
	    		coordsLine = "";
	    	}
	    	
	    	if (ctr != maxLoc-1){
    			coordsLine = coordsLine + routes.get(i).lat +"," + routes.get(i).lng+ '|';
    		} else {
    			coordsLine = coordsLine + routes.get(i).lat +"," + routes.get(i).lng;
    			String url = "https://maps.googleapis.com/maps/api/elevation/json?locations="+ coordsLine +"&key="+DIRECTIONS_API_KEY;
    			JSONArray jsonarray = requestElevation(url);
    			Thread.sleep(200);
				JSONParser parser= new JSONParser();
				for (int x = 0; x < jsonarray.size(); x++) {
					String locations = jsonarray.get(x).toString();
					JSONObject locationMap = (JSONObject)parser.parse(locations);
					JSONObject latlng = (JSONObject)parser.parse(locationMap.get("location").toString());
					double lat = Double.parseDouble(latlng.get("lat").toString());
					double lng = Double.parseDouble(latlng.get("lng").toString());
					double elevation = Double.parseDouble(locationMap.get("elevation").toString());
					output.add(new GeoPoint(lat, lng, elevation));
				}
    		}
    		ctr++;
	    }
	    coordsLine = coordsLine.substring(0,coordsLine.length()-1);
	    String url = "https://maps.googleapis.com/maps/api/elevation/json?locations="+ coordsLine +"&key="+DIRECTIONS_API_KEY;
		JSONArray jsonarray = requestElevation(url);
		JSONParser parser= new JSONParser();
		for (int x = 0; x < jsonarray.size(); x++) {
			String locations = jsonarray.get(x).toString();
			JSONObject locationMap = (JSONObject)parser.parse(locations);
			JSONObject latlng = (JSONObject)parser.parse(locationMap.get("location").toString());
			double lat = Double.parseDouble(latlng.get("lat").toString());
			double lng = Double.parseDouble(latlng.get("lng").toString());
			double elevation = Double.parseDouble(locationMap.get("elevation").toString());
			output.add(new GeoPoint(lat, lng, elevation));
		}
		
		return output;
	    
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
	
	
	
	
	public static JSONArray requestElevation(String url) throws Exception{
		URL obj = new URL(url);
		HttpsURLConnection con = (HttpsURLConnection) obj.openConnection();
		con.setRequestMethod("GET");
 
		int responseCode = con.getResponseCode();
		//System.out.println("\nSending 'GET' request to URL : " + url);
		//System.out.println("Response Code : " + responseCode);
 
		BufferedReader br = new BufferedReader(
		new InputStreamReader(con.getInputStream()));
		String inputLine;
		StringBuffer response = new StringBuffer();
 
		while ((inputLine = br.readLine()) != null) {
			response.append(inputLine);
		}
		br.close();
		
		String resp = response.toString();
		//System.out.println(resp);
		JSONParser parser=new JSONParser();
		JSONObject jsonobj = (JSONObject)parser.parse(resp);
		Iterator iter = jsonobj.entrySet().iterator();
		JSONArray jsonarray = (JSONArray) jsonobj.get("results");
		return jsonarray;
	}
	
	
	public static void writeElevationToFile() throws Exception{
		String filename = LATLNGELEV_FILE;
		String logname = "log.txt";
		writer = new PrintWriter(filename, "UTF-8");
		logwriter = new PrintWriter(logname, "UTF-8");
		File file = new File(COORDS_FILE);
	    BufferedReader in = new BufferedReader(new FileReader(file));
	    String line;
	    int ctr = 1;
	    while ((line = in.readLine()) != null) {
	    	String url = "https://maps.googleapis.com/maps/api/elevation/json?locations="+ line +"&key="+DIRECTIONS_API_KEY;
			try{
				JSONArray jsonarray = requestElevation(url);
				JSONParser parser= new JSONParser();
				for (int i = 0; i < jsonarray.size(); i++) {
					String locations = jsonarray.get(i).toString();
					JSONObject locationMap = (JSONObject)parser.parse(locations);
					JSONObject latlng = (JSONObject)parser.parse(locationMap.get("location").toString());
					writer.println(locationMap.get("elevation")+"|||" + latlng.get("lat") + "|||" + latlng.get("lng"));
				}
	    	} catch (Exception e){
	    		logwriter.println("Error at line " + ctr);
	    		logwriter.println(url);
	    		logwriter.println(e.toString());
	    	}
			ctr++;
			Thread.sleep(200);
		}
		
	    writer.close();
	    logwriter.close();
	}
	
	public static void createFullDataFile() throws Exception{
		File file = new File(LATLNGELEV_FILE_MASTER);
	    BufferedReader in = new BufferedReader(new FileReader(file));
	    String line, line2;
	    File masterfile = new File(STATION_FILE_MASTER);
	    BufferedReader in2 = new BufferedReader(new FileReader(masterfile));
	    File fullDataFile =new File(FULL_DATA_FILE);
	    writer = new PrintWriter(fullDataFile, "UTF-8");
	    int ctr = 0;
	    writer.println("x-coord|||y-coord|||z-coord|||elevation|||lat|||lng|||regprice|||dieselprice|||address|||state|||zip|||uid");
	    while ((line = in.readLine()) != null) {
	    	line2 = in2.readLine();
	    	String[] latlngelev = line.split(Pattern.quote("|||"));
	    	double[] xyz = latlngelevToXYZ(Double.parseDouble(latlngelev[1]), Double.parseDouble(latlngelev[2]), Double.parseDouble(latlngelev[0]));
	    	writer.println(xyz[0]+"|||" + xyz[1]+ "|||"+ xyz[2] + "|||"+latlngelev[0] + "|||" +line2 + "|||" + ctr);
	    	ctr++;
	    }
	  
	    writer.close();
	    
	}
	
	public static void createCoordsFile() throws Exception{
		File file = new File(STATION_FILE_MASTER);
	    BufferedReader in = new BufferedReader(new FileReader(file));
	    String line;
	    int ctr = 0;
	    String filename = COORDS_FILE;
	    writer = new PrintWriter(filename, "UTF-8");
	    String coordsLine = "";
	    while ((line = in.readLine()) != null) {
	    	String[] stationInfo = line.split(Pattern.quote("|||"));
	    	if (ctr == maxLoc){
	    		ctr = 0;
	    		coordsLine = "";
	    	}
	    	
	    	if (ctr != maxLoc-1){
    			coordsLine = coordsLine + stationInfo[0] +"," + stationInfo[1]+ '|';
    		} else{
    			coordsLine = coordsLine + stationInfo[0] +"," + stationInfo[1];
    			writer.println(coordsLine);
    		}
    		ctr++;
	    }
	    writer.println(coordsLine.substring(0,coordsLine.length()-1));
	    writer.close();
	    in.close();
	}
	
	public static void gasStationCrawler() throws Exception{
		String filename = "gasStationList.txt";
		String logname = "log.txt";
		writer = new PrintWriter(filename, "UTF-8");
		logwriter = new PrintWriter(logname, "UTF-8");
		double lat = MIN_US_LAT;
		double lng = MIN_US_LNG;
		String resp = "";
		int ctr = 0;
		while (lat < MAX_US_LAT) {
			while (lng < MAX_US_LNG){
				ctr++;
				lng = lng + 10*LNG_TEN_MILE_OFFSET;
				try{
					resp = getGasStations(lat,lng,50,GasType.reg,SortBy.price);
					writeStationsToFile(resp);
				} catch (Exception e){
					logwriter.println("Error processing coords: " + lat + "," + lng);
					logwriter.println(e.toString());
				}
			}
			lng = MIN_US_LNG;
			lat = lat + 10*LAT_TEN_MILE_OFFSET;
		}
		
		lat = MIN_US_LAT + 5*LNG_TEN_MILE_OFFSET;
		lng = MIN_US_LNG + 5*LNG_TEN_MILE_OFFSET;
		while (lat < MAX_US_LAT) {
			while (lng < MAX_US_LNG){
				ctr++;
				lng = lng + 10*LNG_TEN_MILE_OFFSET;
				try{
					resp = getGasStations(lat,lng,50,GasType.reg,SortBy.price);
					writeStationsToFile(resp);
				} catch (Exception e){
					logwriter.println("Error processing coords: " + lat + "," + lng);
					logwriter.println(e.toString());
					
				}
			}
			lng = MIN_US_LNG;
			lat = lat + 10*LAT_TEN_MILE_OFFSET;
		}
		writer.close();
		logwriter.close();
	}
	
	public static void printLegs(String polyline){
		ArrayList<GeoPoint> coordList = decodePoly(polyline);
		double lastLatitude = coordList.get(0).lat/1E6;
		double lastLongitude = coordList.get(0).lng/1E6;
		double totalDistance = 0;
		for (int i = 1; i < coordList.size(); i++){
			double curLatitude = coordList.get(i).lat/1E6;
			double curLongitude = coordList.get(i).lng/1E6;
			double distance = distance(lastLatitude, lastLongitude ,curLatitude,curLongitude,0,0, Units.miles);
			totalDistance = totalDistance + distance;
			System.out.println("Distance from " + lastLatitude + ", " + lastLongitude + " to " + curLatitude+ ", " + curLongitude + " is " + distance + "miles");
			lastLatitude = curLatitude;
			lastLongitude = curLongitude;
		}
		System.out.println("Total Distance is " + totalDistance + "Total Legs = " + coordList.size());
	}	
	
	
	// Gets all gas stations that are distance miles away from a point defined by latitude, longitude
	private static String getGasStations(double latitude, double longitude, double distance, GasType gastype, SortBy sort) throws Exception {
		 
		String url = "http://devapi.mygasfeed.com/stations/radius/"+latitude+"/"+longitude+"/"+distance+"/"+gastype+"/"+sort+"/"+APIKEY+".json?";
		//System.out.println(url);
		URL obj = new URL(url);
		HttpURLConnection con = (HttpURLConnection) obj.openConnection();
 
		// optional default is GET
		con.setRequestMethod("GET");
 
		int responseCode = con.getResponseCode();
		System.out.println("Sending 'GET' request to URL : " + url);
		System.out.println("Response Code : " + responseCode);
 
		BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
		String inputLine;
		StringBuffer response = new StringBuffer();
 
		while ((inputLine = in.readLine()) != null) {
			response.append(inputLine);
		}
		in.close();
		
		return response.toString();
		//print result
		//System.out.println(resp);
		
	}
	
	
	//Parses a response file in JSON format and dumps the data to a file
	private static void writeStationsToFile(String resp) throws ParseException, FileNotFoundException, UnsupportedEncodingException{
		JSONParser parser=new JSONParser();
		JSONObject jsonobj = (JSONObject)parser.parse(resp);
		Iterator iter = jsonobj.entrySet().iterator();
		JSONArray jsonarray = (JSONArray) jsonobj.get("stations");
		
		for (int i = 0; i < jsonarray.size(); i++){
			String gasStations = jsonarray.get(i).toString();
			JSONObject stationMap = (JSONObject)parser.parse(gasStations);
			String key = stationMap.get("lat") + "||||" + stationMap.get("lng");
			String line = stationMap.get("lat") + "|||"+stationMap.get("lng")+"|||"+stationMap.get("reg_price")+"|||"+
					stationMap.get("diesel_price") + "|||" + stationMap.get("address")+"|||"+stationMap.get("region")+"|||"+stationMap.get("zip");
			
			if (!allStations.containsKey(key)){
				allStations.put(key,1);
				writer.println(line);
			} 
			
		}
	}
	
	private static ArrayList<GeoPoint> getRoute(String startingAddress, String endAddress) throws Exception {
		String url = "https://maps.googleapis.com/maps/api/directions/json?origin=" + startingAddress+"&destination="+endAddress+"&key="+DIRECTIONS_API_KEY;
		System.out.println(url);
		URL obj = new URL(url);
		HttpsURLConnection con = (HttpsURLConnection) obj.openConnection();
 
		// optional default is GET
		con.setRequestMethod("GET");
 
		//add request header
		//con.setRequestProperty("User-Agent", USER_AGENT);
		
		int responseCode = con.getResponseCode();
		System.out.println("\nSending 'GET' request to URL : " + url);
		System.out.println("Response Code : " + responseCode);
 
		BufferedReader in = new BufferedReader(
		        new InputStreamReader(con.getInputStream()));
		String inputLine;
		StringBuffer response = new StringBuffer();
 
		while ((inputLine = in.readLine()) != null) {
			response.append(inputLine);
		}
		in.close();
		
		String resp = response.toString();
		//print result
		//System.out.println(resp);
		String polyline = "";
		JSONParser parser=new JSONParser();
		JSONObject jsonobj = (JSONObject)parser.parse(resp);
		JSONArray jsonarray = (JSONArray) jsonobj.get("routes");
		
		
		for (int i = 0; i < jsonarray.size(); i++){
			//System.out.println(jsonarray.get(i));
			JSONObject routes = (JSONObject)parser.parse(jsonarray.get(i).toString());
			JSONObject points = (JSONObject) routes.get("overview_polyline");
			polyline = points.get("points").toString();
		}
		
		ArrayList<GeoPoint> route = decodePoly(polyline);
		return route;
	}
 
	// HTTP POST request - stub
	private void sendPost() throws Exception {
 
		String url = "https://selfsolve.apple.com/wcResults.do";
		URL obj = new URL(url);
		HttpsURLConnection con = (HttpsURLConnection) obj.openConnection();
 
		//add reuqest header
		con.setRequestMethod("POST");
		con.setRequestProperty("User-Agent", USER_AGENT);
		con.setRequestProperty("Accept-Language", "en-US,en;q=0.5");
 
		String urlParameters = "sn=C02G8416DRJM&cn=&locale=&caller=&num=12345";
 
		// Send post request
		con.setDoOutput(true);
		DataOutputStream wr = new DataOutputStream(con.getOutputStream());
		wr.writeBytes(urlParameters);
		wr.flush();
		wr.close();
 
		int responseCode = con.getResponseCode();
		System.out.println("\nSending 'POST' request to URL : " + url);
		System.out.println("Post parameters : " + urlParameters);
		System.out.println("Response Code : " + responseCode);
 
		BufferedReader in = new BufferedReader(
		        new InputStreamReader(con.getInputStream()));
		String inputLine;
		StringBuffer response = new StringBuffer();
 
		while ((inputLine = in.readLine()) != null) {
			response.append(inputLine);
		}
		in.close();
 
		//print result
		System.out.println(response.toString());
	
	}
	
	private static ArrayList<GeoPoint> decodePoly(String encoded) {

		ArrayList<GeoPoint> poly = new ArrayList<GeoPoint>();
		int index = 0, len = encoded.length();
		int lat = 0, lng = 0;

		while (index < len) {
			int b, shift = 0, result = 0;
			do {
				b = encoded.charAt(index++) - 63;
				result |= (b & 0x1f) << shift;
				shift += 5;
			} while (b >= 0x20);
			int dlat = ((result & 1) != 0 ? ~(result >> 1) : (result >> 1));
			lat += dlat;

			shift = 0;
			result = 0;
			do {
				b = encoded.charAt(index++) - 63;
				result |= (b & 0x1f) << shift;
				shift += 5;
			} while (b >= 0x20);
			int dlng = ((result & 1) != 0 ? ~(result >> 1) : (result >> 1));
			lng += dlng;

			GeoPoint p = new GeoPoint((lat / 1E5),(lng / 1E5));
			poly.add(p);
		}

		return poly;
	}
	
	public static class GeoPoint{
		double lat, lng, elev;
		
		public GeoPoint(double lat, double lng){
			this.lat = lat;
			this.lng = lng;
					
		}
		
		public GeoPoint(double lat, double lng, double elev){
			this.lat = lat;
			this.lng = lng;
			this.elev = elev;
		}
		
		public String toString(){
			return ("Latitude: " + lat + " Longitude: " + lng + " Elevetion: " + elev);
		}
		
	}
	
	
	/*
	 * Calculate distance between two points in latitude and longitude taking
	 * into account height difference. If you are not interested in height
	 * difference pass 0.0. Uses Haversine method as its base.
	 * 
	 * lat1, lon1 Start point lat2, lon2 End point el1 Start altitude in meters
	 * el2 End altitude in meters
	 */
	private static double distance(double lat1, double lon1, double lat2, double lon2,
	        double el1, double el2, Units units) {

	    final int R = 6371; // Radius of the earth

	    Double latDistance = deg2rad(lat2 - lat1);
	    //System.out.println("latDistance: " + latDistance);
	    Double lonDistance = deg2rad(lon2 - lon1);
	    //System.out.println("lonDistance: " + lonDistance);
	    Double a = Math.sin(latDistance / 2) * Math.sin(latDistance / 2)
	            + Math.cos(deg2rad(lat1)) * Math.cos(deg2rad(lat2))
	            * Math.sin(lonDistance / 2) * Math.sin(lonDistance / 2);
	    Double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
	    //System.out.println("c: " + c);
	    double distance = R * c * 1000; // convert to meters
	    //System.out.println("distance: " + distance);
	    double height = el1 - el2;
	    distance = Math.pow(distance, 2) + Math.pow(height, 2);
	    distance = Math.sqrt(distance);
	    
	    switch (units){
	    case km: 	distance = distance/1000;
	    		 	break;
	    case miles: distance = distance/1600;
	    			break;
	    }
	    return distance;
	}

	private static double deg2rad(double deg) {
	    return (deg * Math.PI / 180.0);
	}
	
	private static String googleAPIFormat(String s){
		return s.replaceAll(" ", "%20");
	}

}
