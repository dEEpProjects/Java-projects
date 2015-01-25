


import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.FileChannel;

public class IOUtils {
  private static final int DEFAULT_BUFFER_SIZE = 1024 * 4;

  private IOUtils() {
    // Utility class.
  }

  public static void copyFile(File sourceFile, File destFile) throws IOException {
	  if (!destFile.exists()) {
	    destFile.createNewFile();
	  }
	  FileInputStream fIn = null;
	  FileOutputStream fOut = null;
	  FileChannel source = null;
	  FileChannel destination = null;
	  try {
	    fIn = new FileInputStream(sourceFile);
	    source = fIn.getChannel();
	    fOut = new FileOutputStream(destFile);
	    destination = fOut.getChannel();
	    long transfered = 0;
	    long bytes = source.size();
	    while (transfered < bytes) {
	      transfered += destination.transferFrom(source, 0, source.size());
	      destination.position(transfered);
	    }
	  } finally {
	    if (source != null) {
	      source.close();
	    } else if (fIn != null) {
	      fIn.close();
	    }
	    if (destination != null) {
	      destination.close();
	    } else if (fOut != null) {
	      fOut.close();
	    }
	  }
	}

}