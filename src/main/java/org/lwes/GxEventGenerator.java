package org.lwes;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.lwes.emitter.UnicastEventEmitter;

import java.io.*;
import java.text.NumberFormat;
import java.util.*;

/**
 * @author alexbohr
 * Date: 10/3/12
 */
public class GxEventGenerator {
	private static transient Log log = LogFactory.getLog( GxEventGenerator.class);

	private String campaignsFile = "src/main/resources/MetaMarket_Demand.csv";
	private String appsFile = "src/main/resources/MetaMarket_App.csv";
	private String geosFile = "src/main/resources/MetaMarket_Geo.csv";

	private static ArrayList<HashMap<String, String>> campaigns = new ArrayList<HashMap<String, String>>();
	private static ArrayList<HashMap<String, String>> apps = new ArrayList<HashMap<String, String>>();
	private static ArrayList<HashMap<String, String>> geos = new ArrayList<HashMap<String, String>>();

	public static void main(String[] args){
		if( args.length == 1)   {
			GxEventGenerator gxE = new GxEventGenerator(args[0]);
		}
		else{
			GxEventGenerator gxE = new GxEventGenerator();
		}

	}

	GxEventGenerator(String files){
		String[] fileArray = files.split(",");
		this.campaignsFile = fileArray[0];
		this.appsFile = fileArray[1];
		this.geosFile = fileArray[2];
		initialize();

	}

	GxEventGenerator(){
		  initialize();
	}

	public void initialize(){
		loadValues(campaignsFile, campaigns);
		loadValues(appsFile, apps);
		loadValues(geosFile, geos);
	}

	public void loadValues(String sampleFile, ArrayList<HashMap<String, String>> samples){
		try{
			File f = new File( sampleFile );
			BufferedReader in = new BufferedReader(new FileReader(f));
			String currentLine = in.readLine();
			// Load the keys
			String[] keys = currentLine.split(",");
			// Load each row of values
			while( (currentLine = in.readLine()) != null){
				HashMap<String, String> sample = new HashMap<String, String>();
				String[] values = currentLine.split(",");
				if( values.length != keys.length){
					log.error("Found sample record with incorrect # of fields: "+values.length+" Expected length "+keys.length);
					System.out.println("Found sample record with incorrect # of fields: "+values.length+" Expected length "+keys.length);
					System.out.println("Bad Record: "+currentLine);
					System.exit(84);
				}
				for( int i =0; i< keys.length; i++){
					sample.put(keys[i], values[i]);
				}
				samples.add(sample);
			}

		}
		catch( FileNotFoundException e){
			log.error("Can't find file "+sampleFile+" ERROR: "+e.getMessage());
		}
		catch( IOException e){
			log.error("IOException: "+e.getMessage());
		}
	}

	public LinkedList<Event> createTransaction( int count ) throws EventSystemException, InterruptedException{
		UnicastEventEmitter ue = new UnicastEventEmitter();
		String e_id, g_request_id;
		LinkedList<Event> eventList = new LinkedList<Event>();


		for (int i = 0; i < count; i++) {
			Random random = new Random();
			Set<Map.Entry<String, String>> entries = new HashSet<Map.Entry<String, String>>();
			HashMap<String, String> attributes = new HashMap<String, String>();
			//Pick a random set of attributes for this transaction
			//Get a campaign
			int idx = getRandomInteger(0, campaigns.size()-1, random);
			entries = campaigns.get(idx).entrySet();
			for ( Map.Entry<String,String> entry : entries ){
				attributes.put(entry.getKey(), entry.getValue());
			}
			//Get an app
			idx = getRandomInteger(0, apps.size()-1, random);
			entries = apps.get(idx).entrySet();
			for ( Map.Entry<String,String> entry : entries ){
				attributes.put(entry.getKey(), entry.getValue());
			}
			//Get a geo
			idx = getRandomInteger(0, geos.size()-1, random);
			entries = geos.get(idx).entrySet();
			for ( Map.Entry<String,String> entry : entries ){
				attributes.put(entry.getKey(), entry.getValue());
			}

			// First create a Bid::Response
			e_id = java.util.UUID.randomUUID().toString();
			g_request_id = java.util.UUID.randomUUID().toString();

			NumberFormat n = NumberFormat.getCurrencyInstance(Locale.US);
			String price = n.format(random.nextDouble());
			//long epoch = System.currentTimeMillis()/1000;

			Event evt = ue.createEvent("Bid::Response", false);
			evt.setString("e_id", e_id);
			evt.setString("g_request_id", g_request_id);
			evt.setString("u_ip", "0.0.0.0");
			evt.setString("b1_price", price);
			for(Map.Entry<String,String> entry : attributes.entrySet()){
				evt.setString(entry.getKey(), entry.getValue());
			}
			eventList.add(evt);

			// Next, emit a bid result 50%
			double didWin = random.nextDouble();
			if( didWin > .5){
				Thread.sleep(50); // want to get some difference in the timestamps
				e_id = java.util.UUID.randomUUID().toString();
				evt = ue.createEvent("Bid::Result", false);
				evt.setString("e_id", e_id);
				evt.setString("g_request_id", g_request_id);
				evt.setString("u_ip", "0.0.0.0");
				/*for(Map.Entry<String,String> entry : attributes.entrySet()){
					evt.setString(entry.getKey(), entry.getValue());
				} */
				eventList.add(evt);
				//Next, emit a click for 10% of impressions
				double didClick = random.nextDouble();
				if( didClick > .9){
					Thread.sleep(50); // want to get some difference in the timestamps
					e_id = java.util.UUID.randomUUID().toString();
					evt = ue.createEvent("Click:Received", false);
					evt.setString("e_id", e_id);
					evt.setString("g_request_id", g_request_id);
					evt.setString("u_ip", "0.0.0.0");
					/*for(Map.Entry<String,String> entry : attributes.entrySet()){
						evt.setString(entry.getKey(), entry.getValue());
					} */
					eventList.add(evt);
				}
			}
		}
		return eventList;
	}

	private static int getRandomInteger(int aStart, int aEnd, Random aRandom){
		if ( aStart > aEnd ) {
			throw new IllegalArgumentException("Start cannot exceed End.");
		}
		//get the range, casting to long to avoid overflow problems
		long range = (long)aEnd - (long)aStart + 1;
		// compute a fraction of the range, 0 <= frac < range
		long fraction = (long)(range * aRandom.nextDouble());
		int randomNumber =  (int)(fraction + aStart);
		return randomNumber;
	}
}
