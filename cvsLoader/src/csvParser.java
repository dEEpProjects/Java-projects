import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class csvParser {
	
    public csvParser(String file, String query, String dbUrl, String dbUser, String dbPass, String dp) throws Exception {
        
        BufferedReader fileReader = null;
         
        String modelQuery = query;
        
        // Gestione connessione db
        dbConnector db;
        
		try {
			db = new dbConnector(dbUrl, dbUser, dbPass, file, dp);
		} catch (Exception dbe) {
			throw new Exception();
		}
			
    	System.out.println("Inizio processing del file");
    	
        //Delimiter used in CSV file
        final String DELIMITER = ",";
        try
        {
            String line = "";
            //Create the file reader
            fileReader = new BufferedReader(new FileReader(file));
            
            //System.out.println("Query estratte: ");
            
            //Read the file line by line
            while ((line = fileReader.readLine()) != null)
            {
                //Get all tokens available in line
                String[] tokens = line.split(DELIMITER);
                
                // Preparo query
                query = query.replace("val1", tokens[0]);
                query = query.replace("val2", tokens[1]);
                query = query.replace("val3", tokens[2]);
                query = query.replace("val4", tokens[3]);
                 
                // Effettuo query
                
                //System.out.println(query);
                
                db.insert(query);
                
                // Resetto la query modello
                query = modelQuery;
                
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        } finally {	
        	
        	// Stampo resoconto
        	if(db.getRowFailed() == 0) {
        		System.out.println("*****Tutto perfetto [" + db.getRowSuccess() + " record aggiunti]*****");
        	} else {
        		System.out.println("*****" + db.getRowFailed() + " record non sono stati aggiunti. Guarda il log!*****");
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