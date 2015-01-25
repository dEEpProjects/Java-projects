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
import java.util.Date;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.nio.file.StandardWatchEventKinds.ENTRY_CREATE;
import static java.nio.file.StandardWatchEventKinds.OVERFLOW;


public class Main {
	
	private static String ip;
	private static String dp;
	private static String lp;
	
	private static String regex1;
	private static String regex2;
	private static String regex3;
	private static String regex4;
	private static String regex5;
	
	private static String queryString;
	private static String dbUrl;
	private static String dbUser;
	private static String dbPass;
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		Properties prop = new Properties();
	    try {

	        File jarPath = new File(Main.class.getProtectionDomain().getCodeSource().getLocation().getPath());
	        String propertiesPath = jarPath.getParentFile().getAbsolutePath()+"/preferences.properties";
	        
	        File prefFile = new File(propertiesPath);
	        
	        if(prefFile.exists()) {
	        	FileInputStream is = new FileInputStream(propertiesPath);
	        	prop.load(is);
	        	
	        	System.out.println("Leggo le preferenze dal file di configurazione");
	        	
	        	// Leggo le preferenze dal file di configurazione
		        readPreferences(prop);
		        
		        System.out.println("Mi metto in watch sulla cartella " + ip);
		        
		        // Mi metto in watch nella input folder
		        processEvents();
	        } else {
	        	System.out.println("Problema nella lettura del file di preferenze. Controlla che sia presente nella stessa cartella del jar e riavvia il programma!");
	        	System.exit(0);
	        }
	        
	    } catch (IOException e1) {
	        e1.printStackTrace();
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
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}	
	}
	
	private static void deleteFile(String filename) {
		File file = new File(ip + "/" + filename);
		file.delete();
		
		System.out.println("Cancello il file " + filename);
	}
	
	
	
	private static void readPreferences(Properties prop) {
		// Cerco eventuali anomalie
		if(!validatePreferences(prop)) {
			System.out.println("Problema nelle cartelle settate, controlla la loro esistenza e riavvia il programma!");
			System.exit(0);
		}
		
		// Paths
		ip = prop.getProperty("sourcePath");
		dp = prop.getProperty("destinationPath");
		lp = prop.getProperty("logPath");
		
		// Regexs
		regex1 = prop.getProperty("regex1");
		regex2 = prop.getProperty("regex2");
		regex3 = prop.getProperty("regex3");
		regex4 = prop.getProperty("regex4");
		regex5 = prop.getProperty("regex5");
		
		// Db
		queryString = prop.getProperty("queryString");
		
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
	            
	            //Verifica se il file appena creato è un file di testo.
	            
	            if(filename.toString().matches(regex1) || filename.toString().matches(regex2) || filename.toString().matches(regex3) || filename.toString().matches(regex4) || filename.toString().matches(regex5)) {	
	            	System.out.println(filename + " è un file csv valido");
	            	
	            	
	            	boolean successful = true;
	            	
	            	try {
						csvParser parser = new csvParser(ip + "/" + filename.toString(), queryString, dbUrl, dbUser, dbPass, dp);
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
	             }
	            
	            System.out.println("Attendo il prossimo file...");
	            continue;   
	        }
	         
	        //Applichiamo reset a key in modo da poter ricevere un nuovo evento
	        boolean valid = key.reset();
	         
	        if (!valid) break;
	    }
	}

}
