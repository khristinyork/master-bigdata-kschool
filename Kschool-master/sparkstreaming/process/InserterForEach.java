package spark.process;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;
import com.mongodb.spark.sql.fieldTypes.api.java.Timestamp;
import org.apache.spark.sql.ForeachWriter;
import org.apache.spark.sql.Row;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;


import java.io.Serializable;

public class InserterForEach extends ForeachWriter<Row> implements Serializable{
	private static String Ip = "192.168.1.225";
	private static String dbNameMongo = "mongowdb";
	private static String databaseMysql="wdb";
	private static String collectionName = "tablamedidas";
	private static String tableName = "tablemeasure";
	
	MongoClient mongoclient;
	DBCollection table1;
	DB db;
	BasicDBObject document;
	PreparedStatement preparedStmt;
	Connection con;
	@Override
	public void close(Throwable arg0) {
    try {
			
		}catch(Exception ex) {System.out.println(ex.toString());} 
		finally {
			this.mongoclient.close();
		}
	}

	@Override
	public boolean open(long arg0, long arg1) {
		//siempre true para que escriba
		return true;
	}

	@Override
	public void process(Row fila) {
	         try {
				System.out.println("ROW: " + fila.toString());
				
				 this.mongoclient = new MongoClient(Ip, 27017);
				 this.db = this.mongoclient.getDB(dbNameMongo);
				 this.table1 = this.db.getCollection(collectionName);
				 this.document = new BasicDBObject();
				 
				 Class.forName("com.mysql.jdbc.Driver").newInstance();
				 String sURL = "jdbc:mysql://192.168.1.225:3306/"+databaseMysql;
				 con = DriverManager.getConnection(sURL, "root", "1234");
				 
				 //formateo de fecha
				 int año,dia,mes;
				 String[] fecha=fila.get(7).toString().split("-");
				 año=new Integer(fecha[0]).intValue();
				 mes=new Integer(fecha[1]).intValue();
				 String[] diaaux=fecha[2].split(" ");
				 dia= new Integer(diaaux[0]).intValue();
				 String query = " insert into tablemeasure (idcity, maxtemp, mintemp, avghumed, avgpres,start_window)" + " values (?, ?, ?, ?, ?,?)";
				 preparedStmt = con.prepareStatement(query);
				 preparedStmt.setInt(1, new Integer(fila.get(2).toString()).intValue());
				 preparedStmt.setInt(2, new Integer(fila.get(0).toString()).intValue());
				 preparedStmt.setInt(3, new Integer(fila.get(4).toString()).intValue());
				 preparedStmt.setFloat(4, new Float(fila.get(5).toString()).floatValue());
				 preparedStmt.setFloat(5, new Float(fila.get(6).toString()).floatValue());
				 preparedStmt.setDate(6,new Date(año,mes,dia));
				 preparedStmt.execute();
				 preparedStmt.close();
				 con.close();
					
				//ROW: [291,-26,934985,31,-26,291.0,920.0]
				//datos a insertar en tabla de mongodb llamada  tablaprincipal
				 document.put("maxtemp", fila.get(0));
				 document.put("lat", fila.get(1));
				 document.put("idcity", fila.get(2));
				 document.put("log", fila.get(3));	
				 document.put("mintemp", fila.get(4));
				 document.put("avghumed", fila.get(5));				 
				 document.put("avgpres", fila.get(6));
				 document.put("start_window", fila.get(7));
  			     this.table1.insert(document);
			} catch (Exception e) {				
				e.printStackTrace();
			}finally {
				this.mongoclient.close();
				
			}
	}

}
