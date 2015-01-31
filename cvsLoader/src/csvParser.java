import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.logging.Logger;

public class csvParser {
	// Ricavo il logger generale
	private Logger logger = Logger.getLogger("DayLog"); 
	
    public csvParser(String file, QueryInfo info, String dbUrl, String dbUser, String dbPass, String dp) throws Exception {
        
    	boolean resoconto = true;
    	
        BufferedReader fileReader = null;
         
        String[] modelQueries = info.getQueries();
        
        // Gestione connessione db
        dbConnector db;
        
		try {
			db = new dbConnector(dbUrl, dbUser, dbPass, file, dp);
		} catch (Exception dbe) {
			throw new Exception();
		}
		
    	System.out.println("Inizio processing del file");
    	logger.info("Inizio processing del file");
    	
        //Delimiter used in CSV file
        final String DELIMITER = ",";
        try {
            String line = "";
            //Create the file reader
            fileReader = new BufferedReader(new FileReader(file));
            
            //System.out.println("Query estratte: ");
            String[] queries = info.getQueries();
            
            // caso di una sola query
            if(info.getType() == 1) {
            	//Read the file line by line
                while ((line = fileReader.readLine()) != null) {
                    //Get all tokens available in line
                    String[] tokens = line.split(DELIMITER);

                    String query;
                    for(int k = 0; k < queries.length; k++) {
                    	query = queries[k];
                    	// sostituisco i valori dinamici
                    	for(int j = 0; j < tokens.length; j++){
                    		query = query.replace("val"+(j+1), tokens[j]);
                    	}
                    	// sostituisco l'anno
                    	query = query.replace("year", info.getYear());
                    	                  
                    	// generic query
                        db.executeQuery(query, 2);
                    }
                    
                    // Resetto le queries modello
                    queries = modelQueries;
                }
            } else {// caso di 2 query
            	String query = queries[0];
            	
            	// sostituisco l'anno
            	query = query.replace("year", info.getYear());
            	
            	// Esegui delete query
            	db.executeQuery(query, 1);
            	
            	//Read the file line by line
                while ((line = fileReader.readLine()) != null) {
                    //Get all tokens available in line
                    String[] tokens = line.split(DELIMITER);

                    for(int k = 1; k < queries.length; k++) {
                    	query = queries[k];
                    	// sostituisco i valori dinamici
                    	for(int j = 0; j < tokens.length; j++){
                    		query = query.replace("val"+(j+1), tokens[j]);
                    	}
                    	// sostituisco l'anno
                    	query = query.replace("year", info.getYear());
                    	
                           
                    	// generic query
                        db.executeQuery(query, 2);
                    }
                    
                    
                    // Resetto le queries modello
                    queries = modelQueries;
                }
            }
            
            
        } catch (Exception e) {
            if(e.getMessage().equals("General query failed")) {
            	resoconto = false;
            	System.out.println("Fallimento della query generale di cancellazione... Esco!");
            	logger.info("Fallimento della query generale di cancellazione... Esco!");
            }
        } finally {	
        	if(resoconto) {
        		// Stampo resoconto
            	if(db.getRowFailed() == 0) {
            		System.out.println("*****Tutto perfetto [" + db.getRowSuccess() + " record aggiunti]*****");
            		logger.info("*****Tutto perfetto [" + db.getRowSuccess() + " record aggiunti]*****");
            	} else {
            		System.out.println("*****" + db.getRowFailed() + " record non sono stati aggiunti. Guarda il log!*****");
            		logger.info("*****" + db.getRowFailed() + " record non sono stati aggiunti.*****");
            	}
        	}
        	
        	// Chiudo la connessione col db
        	db.closeConnection();
        	//logger.setUseParentHandlers(false);
        	
            try {
                fileReader.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}