import java.io.File;
import java.io.IOException;
import java.sql.DriverManager;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;
 
public class dbConnector {
	
	Connection connection;
	
	private int rowSuccess = 0;
	public int getRowSuccess() {
		return rowSuccess;
	}

	public int getRowFailed() {
		return rowFailed;
	}

	private int rowFailed = 0;
	
	private Logger logger;
	
	private void setupLogger(String filename, String dp) {
		logger = Logger.getLogger("MyLog");  
	    FileHandler fh;  

	    try {  

	    	DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
			Date date = new Date();
			
			String name = new File(filename).getName();
			String todayDateFile = dp + "/" + dateFormat.format(date);
		
			
			File dir = new File(todayDateFile);
			
			if(!dir.exists()) {
				dir = new File(todayDateFile);
				dir.mkdir();
			}
			
			
			File dir2 = new File(todayDateFile + "/" + name.substring(0, name.length() - 4));
			
			if(!dir2.exists()) {
				dir2 = new File(todayDateFile + "/" + name.substring(0, name.length() - 4));
				dir2.mkdir();
			}
			
	        // This block configure the logger with handler and formatter  
	        fh = new FileHandler(todayDateFile + "/"+name.substring(0, name.length() - 4) +"/log.txt");
	        
	        logger.addHandler(fh);
	        
	        SimpleFormatter formatter = new SimpleFormatter();  
	        
	        fh.setFormatter(formatter);  
	    } catch (SecurityException e) {  
	        e.printStackTrace();  
	    } catch (IOException e) {  
	        e.printStackTrace();  
	    }  

	}
	public dbConnector(String dbUrl, String dbUser, String dbPas, String file, String dp) throws Exception {
		 
		try {
			Class.forName("org.postgresql.Driver");
		} catch (ClassNotFoundException e) {
 
			//System.out.println("Where is your PostgreSQL JDBC Driver? "
					//+ "Include in your library path!");
			
			System.out.println("Problema nel caricamento del driver PostgreSQL JDBC");
			
			throw new Exception("Problema nel caricamento del driver PostgreSQL JDBC");
		}
 
		System.out.println("PostgreSQL JDBC Driver registrato!");
 
		try {
			connection = DriverManager.getConnection(dbUrl, dbUser, dbPas);
		} catch (SQLException e) {
 
			//System.out.println("Connection Failed! Check output console");
			System.out.println("Connessione al db fallita");
			
			//return;
			throw new Exception("Connessione al db fallita");
		}
 
		if (connection != null) {
			//System.out.println("You made it, take control your database now!");
			System.out.println("Connessione al db effettuata con successo!");
		}
		
		// Setup del logger di entità
    	setupLogger(file, dp);
		
	}
	
	public void insert(String query) {
	    Statement stmt = null;
	    
	    try {
	    	
	        connection.setAutoCommit(false);
	    	
	    	stmt = connection.createStatement();
	    	stmt.executeUpdate(query);

	        stmt.close();
	        
	        connection.commit();
	        
	     } catch (Exception e) {
	         //System.err.println( e.getClass().getName()+": "+ e.getMessage());
	         
	         logger.warning(query + " : KO " + "[" + e.getMessage() + "]"); 
	         rowFailed += 1;
	         
	         return;
	     }
	    
	     rowSuccess += 1; 
	     logger.info(query + " - OK");
	    //System.out.println("Records created successfully");
	      
	     //System.out.println("Record inserito correttamente");
}
	
	public void closeConnection() {
		try {
			logger.info("Record OK: " + getRowSuccess() + "  ||  Record KO: " + getRowFailed()); 
			connection.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}