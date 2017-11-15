package proyecto.jsonweather;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import javax.validation.Valid;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;

public class Weather implements Serializable {

	private int lon;
	private int lat;
	private int temp;
	private int pressure;
	private int humidity;
	private int tempMin;
	private int tempMax;
	private int id;
	private int datatime;
	@Valid
	private Map<String, Object> additionalProperties = new HashMap<String, Object>();
	private final static long serialVersionUID = 5306476572902783191L;

	/**
	 * No args constructor for use in serialization
	 * 
	 */
	public Weather() {
	}

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public int getDatatime() {
		return datatime;
	}

	public void setDatatime(int datatime) {
		this.datatime = datatime;
	}

	public int getLon() {
		return lon;
	}

	public void setLon(int lon) {
		this.lon = lon;
	}

	public int getLat() {
		return lat;
	}

	public void setLat(int lat) {
		this.lat = lat;
	}

	public int getTemp() {
		return temp;
	}

	public void setTemp(int temp) {
		this.temp = temp;
	}

	public int getPressure() {
		return pressure;
	}

	public void setPressure(int pressure) {
		this.pressure = pressure;
	}

	public int getHumidity() {
		return humidity;
	}

	public void setHumidity(int humidity) {
		this.humidity = humidity;
	}

	public int getTempMin() {
		return tempMin;
	}

	public void setTempMin(int tempMin) {
		this.tempMin = tempMin;
	}

	public int getTempMax() {
		return tempMax;
	}

	public void setTempMax(int tempMax) {
		this.tempMax = tempMax;
	}

	public Map<String, Object> getAdditionalProperties() {
		return this.additionalProperties;
	}

	public void setAdditionalProperty(String name, Object value) {
		this.additionalProperties.put(name, value);
	}

	@Override
	public String toString() {
		return new ToStringBuilder(this).append("lon", lon).append("lat", lat).append("temp", temp)
				.append("pressure", pressure).append("humidity", humidity).append("tempMin", tempMin)
				.append("tempMax", tempMax).append("additionalProperties", additionalProperties).toString();
	}

	@Override
	public int hashCode() {
		return new HashCodeBuilder().append(lon).append(humidity).append(pressure).append(tempMax)
				.append(additionalProperties).append(temp).append(tempMin).append(lat).toHashCode();
	}

	@Override
	public boolean equals(Object other) {
		if (other == this) {
			return true;
		}
		if ((other instanceof Weather) == false) {
			return false;
		}
		Weather rhs = ((Weather) other);
		return new EqualsBuilder().append(lon, rhs.lon).append(humidity, rhs.humidity).append(pressure, rhs.pressure)
				.append(tempMax, rhs.tempMax).append(additionalProperties, rhs.additionalProperties)
				.append(temp, rhs.temp).append(tempMin, rhs.tempMin).append(lat, rhs.lat).isEquals();
	}

}