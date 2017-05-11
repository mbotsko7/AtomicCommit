import java.rmi.*;
import java.net.*;
import java.util.*;
import java.io.*;
import java.math.BigInteger;
import java.security.*;
import java.nio.file.*;
import java.util.concurrent.ExecutionException;

/*****************************//**
* \class ChordUser The user interface for interacting with Chord
* \brief Provides users with an interface to access and manipulate files on the Chord
**********************************/
public class ChordUser
{
     int port;
    Chord    chord;

    /*****************************//**
    * Create MD5 hash with a name:
    * \param objectName Name of the file you want to hash with
    **********************************/
    private long md5(String objectName)
    {
        try
        {
            MessageDigest m = MessageDigest.getInstance("MD5");
            m.reset();
            m.update(objectName.getBytes());
            BigInteger bigInt = new BigInteger(1,m.digest());
            return Math.abs(bigInt.longValue());
        }
        catch(NoSuchAlgorithmException e)
        {
                e.printStackTrace();

        }
        return 0;
    }

    /*****************************//**
    * Create a chorduser with an MD5 hash name based on port:
    * \param p Port of the Chord
    **********************************/
     public ChordUser(int p) {
         port = p;

         Timer timer1 = new Timer();
         timer1.scheduleAtFixedRate(new TimerTask() {
            @Override
             public void run() {
                 try {
                     long guid = md5("" + port);
                     chord = new Chord(port, guid);
                     try{
                         Files.createDirectories(Paths.get(guid+"/repository"));
                     }
                     catch(IOException e)
                     {
                         e.printStackTrace();

                     }
                     System.out.println("Usage: \n\tjoin <ip> <port>\n\twrite <file>\n\twrite <directory>");
                     System.out.println("\tread <file>\n\tdelete <file>");

                     Scanner scan= new Scanner(System.in);
                     String delims = "[ ]+";
                     String command = "";
                     while (true)
                     {
                         String text= scan.nextLine();
                         String[] tokens = text.split(delims);
                         if (tokens[0].equals("join") && tokens.length == 3) {
                             try {
                                 int portToConnect = Integer.parseInt(tokens[2]);

                                 chord.joinRing(tokens[1], portToConnect);
                             } catch (IOException e) {
                                 e.printStackTrace();
                             }
                         }
                         if (tokens[0].equals("leave")){
                            try{
                                chord.leaveRing();
                            }
                            catch (Exception e){
                                e.printStackTrace();
                            }
                         }
                         if (tokens[0].equals("print")) {
                             chord.Print();
                         }
                         if  (tokens[0].equals("write") && tokens.length == 2) {
                             File file = new File(tokens[1]);
                             if(file.exists()){
                               if(file.isDirectory()){
                                 chord.writeDirectory(tokens[1]);
                               }else{
                                 chord.writeFile(tokens[1]);
                               }
                             }
                         }
                         if  (tokens[0].equals("read") && tokens.length == 2) {
                            chord.readFile(tokens[1]);
                         }
                        if  (tokens[0].equals("delete") && tokens.length == 2) {
                            chord.deleteFile(tokens[1]);
                            // Obtain the chord that is responsable for the file:
                            //  peer = chord.locateSuccessor(guidObject);
                            // where guidObject = md5(fileName)
                            // Call peer.delete(guidObject)
                        }
                        if  (tokens[0].equals("find") && tokens.length == 2) {
                            long guidObject = md5(tokens[1]);
                            ChordMessageInterface peer = chord.locateSuccessor(guidObject);
                            System.out.println(peer.getId());
                            // Obtain the chord that is responsable for the file:
                            //  peer = chord.locateSuccessor(guidObject);
                            // where guidObject = md5(fileName)
                            // Call peer.delete(guidObject)
                        }

                     }
                 }
                 catch(RemoteException e)
                 {
                        System.out.println(e);
                 }
            }
         }, 1000, 1000);
    }

    static public void main(String args[])
    {
        if (args.length < 1 ) {
            throw new IllegalArgumentException("Parameter: <port>");
        }
        try{
            ChordUser chordUser=new ChordUser( Integer.parseInt(args[0]));
            Runtime.getRuntime().addShutdownHook(new Thread() {
                public void run() {
                    System.out.println("Forced to disconnect, now leaving");
                    chordUser.chord.leaveRing();
                }
            });
        }
        catch (Exception e) {
           e.printStackTrace();
           System.exit(1);
        }
     }
}
