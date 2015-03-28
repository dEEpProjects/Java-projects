import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Enumeration;
import java.util.Properties;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.nio.file.StandardWatchEventKinds.ENTRY_CREATE;
import static java.nio.file.StandardWatchEventKinds.OVERFLOW;


public class Main {
	
	private static String ip;
	private static String dp;
	private static String lp;
	
	private static String dbUrl;
	private static String dbUser;
	private static String dbPass;
	
	private static Properties prop;
	
	// Supporto logging generale
	private static Logger logger;
	private static String oldDate;
	private static FileHandler vfh;
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		prop = new Properties();
	    try {

	        File jarPath = new File(Main.class.getProtectionDomain().getCodeSource().getLocation().getPath());
	        String propertiesPath = jarPath.getParentFile().getAbsolutePath()+"/preferences.properties";
	        
	        File prefFile = new File(propertiesPath);
	        
	        if(prefFile.exists()) {
	        	FileInputStream is = new FileInputStream(propertiesPath);
	        	prop.load(is);
	        	
	        	System.out.println("Leggo le preferenze dal file di configurazione");
	        	//logger.info("Leggo le preferenze dal file di configurazione"); 
	        	// Leggo le preferenze dal file di configurazione
		        readPreferences(prop);
		        
		        // Creo il general logger
		        System.out.println("Creo il logger giornaliero");
		        setupGeneralDayLogger();
		        
		        System.out.println("Mi metto in watch sulla cartella " + ip);
		        logger.info("Mi metto in watch sulla cartella " + ip); 
		        
		        // Mi metto in watch nella input folder
		        processEvents();
	        } else {
	        	System.out.println("Problema nella lettura del file di preferenze. Controlla che sia presente nella stessa cartella del jar e riavvia il programma!");
	        	logger.info("Problema nella lettura del file di preferenze. Controlla che sia presente nella stessa cartella del jar e riavvia il programma!");
	        	System.exit(0);
	        }
	        
	    } catch (IOException e1) {
	        e1.printStackTrace();
	    }

	}
	
	private static void setupGeneralDayLogger() {
		logger = Logger.getLogger("DayLog");  
		
	    try {  

	    	DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
			Date date = new Date();
			
			oldDate = dateFormat.format(date).toString();
			
			String todayDateFile = lp + "/" + dateFormat.format(date);
		
			
			File dir = new File(todayDateFile);
			
			if(!dir.exists()) {
				dir = new File(todayDateFile);
				dir.mkdir();
			}
			
	        // This block configure the logger with handler and formatter  
	        vfh = new FileHandler(todayDateFile + "/log.txt", true);
	        
	        logger.addHandler(vfh);
	        
	        SimpleFormatter formatter = new SimpleFormatter();  
	        
	        vfh.setFormatter(formatter); 
	        
	        // For disable console output
	        logger.setUseParentHandlers(false);
	    } catch (SecurityException e) {  
	        e.printStackTrace();  
	    } catch (IOException e) {  
	        e.printStackTrace();  
	    }  
	}
	
	// Crea un nuovo logger se è cambiata la data
	private static void manageLogger() {
		DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
		Date newDate = new Date();
		
		if(!dateFormat.format(newDate).toString().equals(oldDate)) {
			changeFh(dateFormat.format(newDate).toString());
		}
	}
	
	// Cambio il file handler del logger giornaliero
	private static void changeFh(String todayDate) {
		try {  
			
			String todayDateFile = lp + "/" + todayDate;
		
			File dir = new File(todayDateFile);
			
			if(!dir.exists()) {
				dir = new File(todayDateFile);
				dir.mkdir();
			}
			
	        // This block configure the logger with handler and formatter  
			FileHandler nfh = new FileHandler(todayDateFile + "/log.txt",true);
	        
			logger.removeHandler(vfh);
			
	        logger.addHandler(nfh);
	        
	        SimpleFormatter formatter = new SimpleFormatter();  
	        
	        nfh.setFormatter(formatter); 
	        // passaggio di consegne
	        vfh = nfh;
	    } catch (SecurityException e) {  
	        e.printStackTrace();  
	    } catch (IOException e) {  
	        e.printStackTrace();  
	    } 
	}
	
	
	
	private static void moveFile(String filename) {
					
	    DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
		Date date = new Date();
		
		String todayDate = dateFormat.format(date);

		File sourceFile = new File(ip + "/" + filename);
		
		File targetFile = new File(dp + "/" + todayDate + "/" + filename.substring(0, filename.length() - 4) + "/" + filename);
		
		try {
			IOUtils.copyFile(sourceFile, targetFile);
			
			System.out.println("Copio " + sourceFile + " in " + targetFile);
			logger.info("Copio " + sourceFile + " in " + targetFile);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}	
	}
	
	private static void deleteFile(String filename) {
		File file = new File(ip + "/" + filename);
		file.delete();
		
		System.out.println("Cancello il file " + filename);
		logger.info("Cancello il file " + filename);
	}
	
	
	
	private static void readPreferences(Properties prop) {
		// Cerco eventuali anomalie
		if(!validatePreferences(prop)) {
			System.out.println("Problema nelle cartelle settate, controlla la loro esistenza e riavvia il programma!");
			logger.info("Problema nelle cartelle settate, controlla la loro esistenza e riavvia il programma!");
			System.exit(0);
		}
		
		// Paths
		ip = prop.getProperty("sourcePath");
		dp = prop.getProperty("destinationPath");
		lp = prop.getProperty("logPath");
				
		dbUrl = prop.getProperty("dbUrl");
		dbUser = prop.getProperty("dbUser");
		dbPass = prop.getProperty("dbPass");
	}
	
	private static boolean validatePreferences(Properties prop) {
		
		File ft = new File(prop.getProperty("sourcePath"));
		
		if (!ft.exists() || !ft.isDirectory()) {
		   return false;
		}
		
		ft = new File(prop.getProperty("destinationPath"));
		
		if (!ft.exists() || !ft.isDirectory()) {
		   return false;
		}
		
		ft = new File(prop.getProperty("logPath"));
		
		if (!ft.exists() || !ft.isDirectory()) {
		   return false;
		}
		
		return true;
	}
	private static QueryInfo checkFilename(String filename) {
		QueryInfo info = null;
		
		for (Enumeration<?> en = prop.propertyNames(); en.hasMoreElements();) {
			
			 String key = (String) en.nextElement();
			 // bisogna escludere le altre chiavi!!!
			 if(key.startsWith("regex")) {
				 // se è una regex del nome
					if(filename.matches(prop.getProperty(key))) {
						 
						 String[] queries = prop.getProperty("queries" + key.substring(5)).split("&");
						 
						 try {
							 info = new QueryInfo(filename, queries);
						 } catch (Exception e) {
							 System.out.println("Problema nella estrazione della data! Riavvia il programma!");
							 logger.info("Problema nella estrazione della data! Riavvia il programma!");
							 System.exit(0);
						 }
						 
						 return info;
						
					 }			 
				 }
		}
			
		return null;
	}
	
	public static void processEvents() throws IOException {
		
		WatchService watcher = FileSystems.getDefault().newWatchService();
		
		Path path = Paths.get(ip);
		
		path.register(watcher, ENTRY_CREATE);
		
	    while(true)
	    {
	        //Si attende che arrivi un nuovo evento CREATE dalla directory registrata
	        WatchKey key;
	         
	        try {
	            key = watcher.take();
	        } catch (InterruptedException x) {
	            return;
	        }
	         
	        for (WatchEvent<?> event: key.pollEvents()) {
	            WatchEvent.Kind kind = event.kind();
	            
	            if (kind == OVERFLOW) continue; 
	             
	            //Recupero del nome del file dal contesto.
	            WatchEvent<Path> ev = (WatchEvent<Path>)event;
	             
	            Path filename = ev.context();
	            
	            System.out.println(filename + " è stato aggiunto a " + ip);
	            logger.info(filename + " è stato aggiunto a " + ip);
	            
	            //Verifica se il file appena creato è un file di testo.
	            
	            
	            QueryInfo infoToPass = checkFilename(filename.toString());
	            
	            if(infoToPass != null) {	
	            	System.out.println(filename + " è un file csv valido");
	            	logger.info(filename + " è un file csv valido");
	            	
	            	manageLogger();
	            	
	            	boolean successful = true;
	            	
	            	try {
						csvParser parser = new csvParser(ip + "/" + filename.toString(), infoToPass, dbUrl, dbUser, dbPass, dp);
					} catch (Exception pe) {
						// TODO Auto-generated catch block
						//e.printStackTrace();
						
						successful = false;
					}
	            	
		            if(successful) {
		            	moveFile(filename.toString());
			            deleteFile(filename.toString());
		            } else {
		            	deleteFile(filename.toString());
		            }
		            
	             } else {
	                System.out.format("Il file '%s' non è un file valido.%n", filename);
	                logger.info("Il file " + filename + " non è un file valido.%n");
	             }
	            
	            System.out.println("Attendo il prossimo file...");
	            logger.info("Attendo il prossimo file...\n\n");
	            
	        }
	         
	        //Applichiamo reset a key in modo da poter ricevere un nuovo evento
	        boolean valid = key.reset();
	         
	        if (!valid) break;
	    }
	}

}
