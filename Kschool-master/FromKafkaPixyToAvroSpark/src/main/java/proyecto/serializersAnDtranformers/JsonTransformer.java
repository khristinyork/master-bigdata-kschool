package proyecto.serializersAnDtranformers;

import java.util.Iterator;

//import org.codehaus.jettison.json.JSONException;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import processing.core.*;
import proyecto.jsonweather.Weather;

public class JsonTransformer {

	public static Weather transformerToweatherSchema(String s0, Weather w) {
		try {

			processing.data.JSONObject mainObject = new processing.data.JSONObject();
			processing.data.JSONObject obj3 = mainObject.parse(s0);
			Iterator<String> itkeys = obj3.keys().iterator();

			while (itkeys.hasNext()) {
				String key = itkeys.next();

				if (key.equals("temp")) {
					if (obj3.isNull(key))
						w.setTemp(0);
					else {
						int temp = obj3.getInt("temp");
						w.setTemp(temp);
					}
				} else if (key.equals("lon")) {
					if (obj3.isNull(key))
						w.setLon(0);
					else {
						int lon = obj3.getInt("lon");
						w.setLon(lon);
					}
				} else if (key.equals("lat")) {
					if (obj3.isNull(key))
						w.setLat(0);
					else {
						int lat = obj3.getInt("lat");
						w.setLat(lat);
					}
				} else if (key.equals("pressure")) {
					if (obj3.isNull(key))
						w.setPressure(0);
					else {
						int pressure = obj3.getInt("pressure");
						w.setPressure(pressure);
					}

				} else if (key.equals("humidity")) {
					if (obj3.isNull(key))
						w.setHumidity(0);
					else {
						int humidity = obj3.getInt("humidity");
						w.setHumidity(humidity);
					}
				} else if (key.equals("temp_min")) {
					if (obj3.isNull(key))
						w.setTempMin(0);
					else {
						int temp_min = obj3.getInt("temp_min");
						w.setTempMin(temp_min);
					}
				} else if (key.equals("temp_max")) {
					if (obj3.isNull(key))
						w.setTempMax(0);
					else {
						int temp_max = obj3.getInt("temp_max");
						w.setTempMax(temp_max);
					}
				} else if (key.equals("id")) {
					if (obj3.isNull(key))
						w.setId(0);
					else {
						int id = obj3.getInt("id");
						w.setId(id);
					}
				} else if (key.equals("datetime")) {
					if (obj3.isNull(key))
						w.setDatatime(0);
					else {
						int datetime = obj3.getInt("datetime");
						w.setDatatime(datetime);
					}
				}
			}
		} catch (

		Exception e) {
			e.printStackTrace();
			System.out.println("Parser :" + e.getMessage());
		}
		return w;
	}

}
