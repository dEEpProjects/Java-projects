import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.logging.Logger;

public class csvParser {
	// Ricavo il logger generale
	private Logger logger = Logger.getLogger("DayLog"); 
	
	// Massimo numero di righe da processare
	public static final int maxRows = 20000;
	
	private String normalize(String input) {
		
		String temp = input;
		// rimuovo il primo singolo apice se presente
		if(temp.startsWith("'")) {
			temp = temp.substring(1);
		}
		// supporto gli accenti
		temp = temp.replace("'", "''");
		
		return temp;
	}
	
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
    	
    	int couTest = 0;
    	
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

                    //System.out.println(tokens.length);
        			//for(int h = 0; h < 92; h++) System.out.println(tokens[h]);
                    
                    //if(tokens.length != 92) System.out.println(tokens.length);
                    
                    String query;
                    for(int k = 0; k < queries.length; k++) {
                    	query = queries[k];
                    	// sostituisco i valori dinamici
                    	for(int j = 1, seq = 1; j < tokens.length; j++, seq++) {
                    		//System.out.println(tokens[j+1]);
                    		// logica campi multipli inizianti con " in singolo
                    		if(tokens[j].contains("\"")) {
                    			
                    			// ne contiene di più (caso particolare)
                    			if(tokens[j].split("\"", -1).length-1 > 1) {
                    				System.out.println("Caso particolare");
                    				
                    				// normalizzo il campo
                    				tokens[j] = normalize(tokens[j]);
                    				
                    				// rimuovo gli apici singoli
                            		//tokens[j] = tokens[j].replace("'", "");
                            		
                            		tokens[j] = tokens[j].replace("\"", "");
                            		
                            		query = query.replaceFirst("val"+seq, tokens[j]);
                    			} else {
                    				//j++;
                        			System.out.println("Double quote detected!!!\n\n\n\n");
                        			
                        			int oldj = j;
                        			
                        			// rimuovo gli apici doppi
                            		tokens[j] = tokens[j].replace("\"", "");
                      			
                            		// rimuovo gli apici singoli
                            		//tokens[j] = tokens[j].replace("'", "");
                            		
                            		// normalizzo il campo
                    				tokens[j] = normalize(tokens[j]);
                    				
                        			String accumulo = tokens[j];
                        			                    			
                        			j++;
                        			
                        			while(!tokens[j].contains("\"")) {
                        				tokens[j] = normalize(tokens[j]);
                        				
                        				accumulo += tokens[j];
                        				
                        				//tokens[j] = tokens[j].replace("'", "");
                        				// normalizzo il campo
                        				
                        				j++;
                        			}
                        			
                        			tokens[j] = tokens[j].replace("\"", "");
                        			//tokens[j] = tokens[j].replace("'", "");
                        			// normalizzo il campo
                    				tokens[j] = normalize(tokens[j]);
                        			accumulo += tokens[j];
                        			
                        			query = query.replaceFirst("val"+oldj, accumulo);
                        			//System.out.println(accumulo);
                        			//System.out.println(query);
                        			
                        			//return;
                    			}

                    		} else {
                    			// rimuovo gli apici singoli
                        		//tokens[j] = tokens[j].replace("'", "");
                    			//normalizzo il campo
                    			tokens[j] = normalize(tokens[j]);
                        		
                        		query = query.replaceFirst("val"+seq, tokens[j]);
                    		}
                    	}
                    	
                    	System.out.println(query);
                    	// sostituisco l'anno
                    	//query = query.replace("year", info.getYear());
                    	                  
                    	// la prima riga viene esclusa perchè non contiene dati ma la struttura
                    	if(couTest > 0) {
                    		// generic query
                    		db.executeQuery(query, 2);
                    	}
                    	
                    }
                    
                    // Resetto le queries modello
                    queries = modelQueries;
                    
                    couTest++;
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
                    		// rimuovo gli apici singoli
                    		tokens[j] = tokens[j].replace("'", "");
                    		
                    		// logica campi multipli inizianti con " in singolo
                    		if(tokens[j].contains("\"")) {
                    			System.out.println("Cazzo!!!\n\n\n");
                    			return;
                    		}
                    		
                    		
                    		
                    		query = query.replace("val"+(j+1), tokens[j]);
                    	}
                    	// sostituisco l'anno
                    	//query = query.replace("year", info.getYear());
                    	
                           
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