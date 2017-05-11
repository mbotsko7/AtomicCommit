import java.io.*;
import java.nio.*;

public class FileStream extends InputStream implements Serializable {
  
    private int currentPosition;
    private byte[] byteBuffer;
    private int size;
    public  FileStream(String pathName) throws FileNotFoundException, IOException    {
      File file = new File(pathName);
      size = (int)file.length();
      byteBuffer = new byte[size];
      
      FileInputStream fileInputStream = new FileInputStream(pathName);
      int i = 0;
      while (fileInputStream.available()> 0)
      {
	byteBuffer[i++] = (byte)fileInputStream.read();
      }
      fileInputStream.close();	
      currentPosition = 0;	  
    }
    
    public  FileStream() throws FileNotFoundException    {
      currentPosition = 0;	  
    }
    
    public int read() throws IOException
    {
 	if (currentPosition < size)
 	  return (int)byteBuffer[currentPosition++];
 	return 0;
    }
    
    public int available() throws IOException
    {
	return size - currentPosition;
    }
}