package websocket.echo;

import java.io.IOException;
import java.io.OutputStream;
import java.io.Writer;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.MissingResourceException;
import java.util.ResourceBundle;
import java.util.Timer;
import java.util.TimerTask;

import javax.websocket.EndpointConfig;
import javax.websocket.OnClose;
import javax.websocket.OnMessage;
import javax.websocket.OnOpen;
import javax.websocket.PongMessage;
import javax.websocket.Session;
import javax.websocket.server.ServerEndpoint;

@ServerEndpoint("/websocket/feeder")
public class Feeder {
    private Writer writer;
    private OutputStream stream;
    private Session session;
    private static Map<Object, Session> mapConnections = new HashMap<Object, Session>();
    public static int sequence = 0;
    public static ResourceBundle rb = null;
    public static ResourceBundle strings = null;
    public static DatabaseHandler db;
    
    public Feeder() {    	
	try {
	    rb = ResourceBundle.getBundle("Feeder");
	    strings = ResourceBundle.getBundle("DebateControlSystemStrings");
	} catch (MissingResourceException e) {
	}
    	new Notifier();
    	db = new DatabaseHandler(rb);
    }
    
    @OnOpen
    public void onOpen(Session session, EndpointConfig config) throws IOException {
    	this.session = session;
    	mapConnections.put(session.getId(), session);
    	String json = "{ \"count\" : \"" + mapConnections.size() + "\"}"; 
    	broadcastToAll(json);
    }
    
    @OnMessage
    public void echoTextMessage(Session session, String message, boolean last) throws IOException {
        if (writer == null) {
            writer = session.getBasicRemote().getSendWriter();
        }
        writer.write(dateTimeString(System.currentTimeMillis()) + " " + message);
        if (last) {
            writer.close();
            writer = null;
        }
    }
            
    @OnMessage
    public void echoBinaryMessage(byte[] msg, Session session, boolean last) throws IOException {
        if (stream == null) {
            stream = session.getBasicRemote().getSendStream();
        }
        stream.write(msg);
        stream.flush();
        if (last) {
            stream.close();
            stream = null;
        }
    }

    @OnMessage
    public void echoPongMessage(PongMessage pm) {
    }

    @OnClose
    public void close(Session session) {
    	// close();
    }
    
    public static String dateTimeString(long milliseconds) {
    	SimpleDateFormat sdf = new SimpleDateFormat("dd.MM.yyyy HH:mm:ss");
    	Date resultdate = new Date(milliseconds);
    	return sdf.format(resultdate);
    }
    
    public static void broadcast(String key, String message) {
    	Session session = mapConnections.get(key);
    	if (session != null) {
    		try {
    			session.getBasicRemote().sendText(message);
    		} catch (IOException e) {
    			try {
    				session.close();
    			} catch (IOException e1) {
    			}
    		}
    	}
    }

    public static void broadcastToAll(String message) {
    	for(Session session : mapConnections.values()) {
    		if(session != null) {
    			try {
    				session.getBasicRemote().sendText(dateTimeString(System.currentTimeMillis()) + " " +
    					message + " sequence " + sequence);
    			} catch (IOException e) {
    				try {
    					session.close();
    				} catch (IOException ee) {
    				}
    			}
    		}
    	}
    }

    public class Notifier extends TimerTask{
    	 
    	 public Notifier() {
    	     Timer timer = new Timer();
    	     timer.schedule(this, 5000, 5000);
    	 }
    	  
    	 @Override
    	 public void run() {
    		 Feeder.sequence++;
    		 // String json = "{ \"message\" : \"Database table is updated, Please refresh to get updated records.\"}";
    		 String json = createJSON(1);
    		 // Feeder.broadcastToAll(json);
    		 Feeder.broadcastToAll(json);
    	 }

    	// public void createJSON(int event, PrintWriter out) {
        public String createJSON(int event) {
    		Statement dbStmt = null;
			Statement dbStmtRemark = null;;
    		ResultSet rsAgenda = null;
    		ResultSet rsPost = null;
    		ResultSet rsRemark = null;
   			String jsonString = "[ ";
   			boolean first = true;
    			
    		try {
    			String sqlAgenda = "SELECT * from reservations AS re, delegates AS de WHERE re.event = " + event +
    					" AND re.status = 1 AND re.reservation_type = 3 AND de.id = re.delegate" +
    					" ORDER BY re.added_dt, de.delegate_number";
    			dbStmt = db.getConnection().createStatement();
    			if (dbStmt.execute(sqlAgenda)) {
    				rsAgenda = dbStmt.getResultSet();
    				while (rsAgenda.next()) {
    					jsonString = jsonString + "{\"delegate_number\": " + rsAgenda.getString("delegate_number") + ", " +
    							"\"name_last\": \"" + rsAgenda.getString("name_last") + "\", " +
    							"\"name_first\": \"" + rsAgenda.getString("name_first") +
    							"\", \"reservation_type\": " +
    							rsAgenda.getString("reservation_type") + "}";
    					if (rsAgenda.isLast()) {
    					} else {
    						jsonString = jsonString + ", ";
    					}
    				}
    			}
    			rsAgenda.close();
    			dbStmt.close();
    			String sqlPost = "SELECT * from reservations AS re, delegates AS de WHERE re.event = " + event +
    					" AND re.status = 1 AND re.reservation_type = 1 AND de.id = re.delegate" +
    					" ORDER BY re.added_dt, de.delegate_number";
    			dbStmt = db.getConnection().createStatement();
    			if (dbStmt.execute(sqlPost)) {
    				rsPost = dbStmt.getResultSet();
    				while (rsPost.next()) {
    					jsonString = jsonString + "{\"delegate_number\": " + rsPost.getString("delegate_number") + ", " +
    							"\"name_last\": \"" + rsPost.getString("name_last") + "\", " +
    							"\"name_first\": \"" + rsPost.getString("name_first") +
    							"\", \"reservation_type\": " +
    							rsPost.getString("reservation_type") + "}";
    					if (hasRemarks(rsPost.getInt("re.id"))) {
    						String sqlRemark = "SELECT * from reservations AS re, delegates AS de WHERE re.event = " + event +
    							" AND re.status = 1 AND re.reservation_type = 2 AND re.reservation_to = " +
    							rsPost.getInt("re.id") + " AND de.id = re.delegate" +
    							" ORDER BY re.added_dt, de.delegate_number";
    						dbStmtRemark = db.getConnection().createStatement();
    						if (dbStmtRemark.execute(sqlRemark)) {
    							rsRemark = dbStmtRemark.getResultSet();
    							while (rsRemark.next()) {
    								jsonString = jsonString + ", {\"delegate_number\": " + rsRemark.getString("delegate_number") + ", " +
    										"\"name_last\": \"" + rsRemark.getString("name_last") + "\", " +
    										"\"name_first\": \"" + rsRemark.getString("name_first") +
    										"\", \"reservation_type\": " +
    										rsRemark.getString("reservation_type") + "}";
    							}
    						}
    						rsRemark.close();
    						dbStmtRemark.close();
   						}
    					if (rsPost.isLast()) {
    					} else {
    						jsonString = jsonString + ", ";
    					}
    				}
    			}
    			rsPost.close();
    			dbStmt.close();
   				jsonString = jsonString + " ]";
   				// out.println(jsonString);
    		} catch (SQLException e) {
    			// out.println("SQLException: " + e.getMessage());
    			e.printStackTrace();
   			}
            return jsonString;
   		}
    }

	public boolean hasRemarks(int id) {
		boolean has = false;
		Statement dbStmt2 = null;
		ResultSet rs2 = null;
		String sql = "SELECT * from reservations WHERE reservation_to = " + id;

		try {
			dbStmt2 = db.getConnection().createStatement();
			if (dbStmt2.execute(sql)) {
    			rs2 = dbStmt2.getResultSet();
    			while (rs2.next()) {
    				has = true;
    				break;
    			}
			}
			dbStmt2.close();
			rs2.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}		
		return has;
	}
	
}
