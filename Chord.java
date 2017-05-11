import javax.swing.tree.TreeNode;
import java.rmi.*;
import java.rmi.registry.*;
import java.rmi.server.*;
import java.net.*;
import java.util.*;
import java.io.*;
import java.math.BigInteger;
import java.security.*;

import static java.lang.System.in;

/*****************************//**
* \class Chord Chord class which will handle all of the methods that are called remotely
* \brief It implements the server
**********************************/
public class Chord extends java.rmi.server.UnicastRemoteObject implements ChordMessageInterface
{
    public static final int M = 2;

    Registry registry;    // rmi registry for lookup the remote objects.
    ChordMessageInterface successor;
    ChordMessageInterface predecessor;
    ChordMessageInterface[] finger;
    int nextFinger;
    long guid;   		// GUID (i)
    /***************************************
     * Begin Atomic Commit
     */



    public boolean canCommit(Transaction trans){
        if(1 == 1){
            trans.setVote(true);
            File f = new File("./tmp/"+trans.getTransactionId());
            try{
                OutputStream ostream = new FileOutputStream(f);
                ObjectOutputStream tstream = new ObjectOutputStream(ostream);
                tstream.writeObject(trans);
            }
            catch (Exception e){
                e.printStackTrace();
            }
            return true;
        }
        else{
            trans.setVote(false);
            return false;
        }
    }

    public void doCommit(Transaction trans) {
        if(trans.getOp() == Transaction.Operation.DELETE){
            try {
                delete(trans.getGuid());

            } catch (RemoteException e) {
                e.printStackTrace();
            }
        }
        else{
            try {
                put(trans.getGuid(), trans.getFileStream());
            } catch (RemoteException e) {
                e.printStackTrace();
            }
        }
        haveCommitted(trans, guid);
    }

    public void doAbort(Transaction trans){
        File f = new File("./tmp/"+trans.getTransactionId());
        if(f.exists()) //in case this participant said no
            f.delete();
    }

    public void haveCommitted(Transaction trans, long participant){

    }

    public boolean getDecision(Transaction trans){
        try {
            //ChordMessageInterface c = locateSuccessor(trans.getCoordinator());
            File f = new File("./tmp/"+trans.getTransactionId());
            try{
                InputStream ostream = new FileInputStream(f);
                ObjectInputStream tstream = new ObjectInputStream(ostream);
                Transaction t = (Transaction)tstream.readObject();
                return t.getVote();
            }
            catch (Exception e){
                e.printStackTrace();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }

    public void writeFile(String fileName){
        try{
            boolean vote = true;
            //tally votes
            for(int i = 0; i < 3; i++){
                long id = md5(fileName+i+1);
                ChordMessageInterface c = locateSuccessor(id);
                Transaction t = new Transaction( guid, id, new FileStream(fileName), (int)(Math.random()*1000), Transaction.Operation.WRITE);
                if(c.canCommit(t) == false)
                    vote = false;
            }
            //if yes
            if(vote){
                for(int i = 0; i < 3; i++){
                    long id = md5(fileName+i+1);
                    ChordMessageInterface c = locateSuccessor(id);
                    c.doCommit(new Transaction(guid, id, new FileStream(fileName), (int)(Math.random()*1000), Transaction.Operation.WRITE));

                }
            }
            else{ //if no
                for(int i = 0; i < 3; i++){
                    long id = md5(fileName+i+1);
                    ChordMessageInterface c = locateSuccessor(id);
                    c.doAbort(new Transaction(guid, id, new FileStream(fileName), (int)(Math.random()*1000), Transaction.Operation.WRITE));

                }
            }


        }catch(IOException e){
            e.printStackTrace();
        }
    }

    public void writeDirectory(String directoryName){
        File f = new File(".");
        for(File file : f.listFiles()){
            writeFile(file.getAbsolutePath());

        }
    }

    public void readFile(String fileName){
        try{
            long guidObject = md5(fileName);
            String path = "./"+this.guid+"/"+fileName;
            ChordMessageInterface peer = locateSuccessor(guidObject);
            InputStream s = peer.get(guidObject);
            FileOutputStream output = new FileOutputStream(path);
            while (s.available() > 0){
                output.write(s.read());
            }
            output.close();
        }catch(RemoteException e){
            System.out.println(e);
        }catch(IOException e){
            e.printStackTrace();
        }
    }

    public void deleteFile(String fileName){
        try{
            boolean vote = true;
            //tally votes
            for(int i = 0; i < 3; i++){
                long id = md5(fileName+i+1);
                ChordMessageInterface c = locateSuccessor(id);
                Transaction t = new Transaction( guid, id, new FileStream(fileName), System.currentTimeMillis(), Transaction.Operation.DELETE);
                if(c.canCommit(t) == false)
                    vote = false;
            }
            //if yes
            if(vote){
                for(int i = 0; i < 3; i++){
                    long id = md5(fileName+i+1);
                    ChordMessageInterface c = locateSuccessor(id);
                    c.doCommit(new Transaction(guid, id, new FileStream(fileName), System.currentTimeMillis(), Transaction.Operation.DELETE));

                }
            }
            else{ //if no
                for(int i = 0; i < 3; i++){
                    long id = md5(fileName+i+1);
                    ChordMessageInterface c = locateSuccessor(id);
                    c.doAbort(new Transaction(guid, id, new FileStream(fileName), System.currentTimeMillis(), Transaction.Operation.DELETE));

                }
            }


        }catch(IOException e){
            e.printStackTrace();
        }

    }

    /***************************************
     * End Atomic Commit
     */
     /*****************************//**
     * Create MD5 hash with a name:
     * \param objectName Name of the file you want to hash with
     **********************************/
    private long md5(String objectName){
       try{
           MessageDigest m = MessageDigest.getInstance("MD5");
           m.reset();
           m.update(objectName.getBytes());
           BigInteger bigInt = new BigInteger(1,m.digest());
           return Math.abs(bigInt.longValue());
       }catch(NoSuchAlgorithmException e){
               e.printStackTrace();
       }
       return 0;
    }


    /*****************************//**
    * Lookup a Chord based on IP and Port:
    * \param ip IP of Chord you are looking for
    * \param port Port of Chord you are looking for
    **********************************/
    public ChordMessageInterface rmiChord(String ip, int port)
    {
        ChordMessageInterface chord = null;
        try{
            Registry registry = LocateRegistry.getRegistry(ip, port);
            chord = (ChordMessageInterface)(registry.lookup("Chord"));
        } catch (RemoteException | NotBoundException e){
            e.printStackTrace();
        }
        return chord;
    }

    /*****************************//**
    * Look for Chord with ID of key in close proximity:
    * \param key Key of chord you are looking for
    * \param key1 Lower range of keys to look in
    * \param key2 Upper range of keys to look in
    **********************************/
    public Boolean isKeyInSemiCloseInterval(long key, long key1, long key2)
    {
       if (key1 < key2)
           return (key > key1 && key <= key2);
      else
          return (key > key1 || key <= key2);
    }

    public Boolean isKeyInOpenInterval(long key, long key1, long key2)
    {
      if (key1 < key2)
          return (key > key1 && key < key2);
      else
          return (key > key1 || key < key2);
    }

    /*****************************//**
    * Write a file into the repository:
    * \param guidObject GUID of Chord that holds the file
    * \param stream InputStream that will be passing the file
    **********************************/
    public void put(long guidObject, InputStream stream) throws RemoteException {
	 //TODO Store the file at ./guid/repository/guid
      try {
          String fileName = "./"+guid+"/repository/" + guidObject;
          FileOutputStream output = new FileOutputStream(fileName);
          while (stream.available() > 0)
              output.write(stream.read());
          output.close();
      }
      catch (IOException e) {
          System.out.println(e);
      }
    }

    /*****************************//**
    * Get a file located in a repository:
    * \param guidObject GUID of Chord that contains the file
    **********************************/
    public InputStream get(long guidObject) throws RemoteException {
	 FileStream file = null;
     try{
         String fileName =  "./"+guid+"/repository/" + guidObject;
         file = new FileStream(fileName);
     }
     catch (IOException e) {
         System.out.println(e);
     }
         //TODO get  the file ./port/repository/guid
        return file;
    }

    /*****************************//**
    * Remove a file from a directory:
    * \param guidObject GUID of Chord that contains the file
    **********************************/
    public void delete(long guidObject) throws RemoteException {
          //TODO delete the file ./port/repository/guid
        try{
            File fileName =  new File("./"+guid+"/repository/" + guidObject);

            fileName.delete();
        }
        catch (Exception e) {
            System.out.println(e);
        }
    }
    /*****************************//**
    * Returns the GUID of the current Chord:
    **********************************/
    public long getId() throws RemoteException {
        return guid;
    }

    /*****************************//**
    * Returns true if the current Chord is active:
    **********************************/
    public boolean isAlive() throws RemoteException {
	    return true;
    }

    /*****************************//**
    * Returns the predecessor as a Chord:
    **********************************/
    public ChordMessageInterface getPredecessor() throws RemoteException {
	    return predecessor;
    }

    /*****************************//**
    * Find successor of a certain Chord:
    * \param key GUID of Chord that you want the successor of
    **********************************/
    public ChordMessageInterface locateSuccessor(long key) throws RemoteException {
	    if (key == guid)
            throw new IllegalArgumentException("Key must be distinct that  " + guid);
	    if (successor.getId() != guid)
	    {
	      if (isKeyInSemiCloseInterval(key, guid, successor.getId()))
	        return successor;
	      ChordMessageInterface j = closestPrecedingNode(key);

          if (j == null)
	        return null;
	      return j.locateSuccessor(key);
        }
        return successor;
    }

    /*****************************//**
    * Predecessor of a Chord you want:
    * \param key GUID of Chord that you want the predecessor of
    **********************************/
    public ChordMessageInterface closestPrecedingNode(long key) throws RemoteException {
        //TODO
        return predecessor; //return successor;

    }


    /*****************************//**
    * Leave the Chord ring by passing all files in repository and breaking connections:
    **********************************/
    public void leaveRing(){
        try{
//            ChordMessageInterface pred = getPredecessor();
//            ChordMessageInterface succ = locateSuccessor(guid);
            successor.notify(predecessor);

            fixFingers();
            stabilize();

            String path = "./"+guid+"/repository";
            File[] f = new File(path).listFiles();
            for (File myfile:
                 f) {
                Long longislong = Long.parseLong(myfile.getName());
                successor.put(longislong, get(longislong));

            }


        }
        catch (Exception e){
            e.printStackTrace();
        }
    }

    /*****************************//**
    * Join a Chord ring at a specified IP and port and initialize communication with them:
    * \param ip IP of ring to connect to
    * \param port Port of ring to connect to
    **********************************/
    public void joinRing(String ip, int port)  throws RemoteException {
        try{
            System.out.println("Get Registry to joining ring");
            Registry registry = LocateRegistry.getRegistry(ip, port);
            ChordMessageInterface chord = (ChordMessageInterface)(registry.lookup("Chord"));
            predecessor = null;
            successor = chord.locateSuccessor(this.getId());
            System.out.println("Joining ring");
        }
        catch(RemoteException | NotBoundException e){
            successor = this;
        }
    }

    /*****************************//**
    * Find the next successor apart from the immediate neighbor:
    **********************************/
    public void findingNextSuccessor()
    {
        int i;
        successor = this;
        for (i = 0;  i< M; i++)
        {
            try
            {
                if (finger[i].isAlive())
                {
                    successor = finger[i];
                }
            }
            catch(RemoteException | NullPointerException e)
            {
                finger[i] = null;
            }
        }
    }

    /*****************************//**
    * Run to make sure successors and predecessors for every Chord are correct:
    **********************************/
    public void stabilize() {
      try {
          if (successor != null)
          {
              ChordMessageInterface x = successor.getPredecessor();

              if (x != null && x.getId() != this.getId() && isKeyInOpenInterval(x.getId(), this.getId(), successor.getId()))
              {
                  successor = x;
              }
              if (successor.getId() != getId())
              {
                  successor.notify(this);
              }
          }
      } catch(RemoteException | NullPointerException e1) {
          findingNextSuccessor();

      }
    }

    /*****************************//**
    * Reach a Chord to assign them a new predecessor:
    * \param j An interface to the Chord you want to modify
    **********************************/
    public void notify(ChordMessageInterface j) throws RemoteException {
         if (predecessor == null || (predecessor != null
                    && isKeyInOpenInterval(j.getId(), predecessor.getId(), guid)))
	 // TODO
	 //transfer keys in the range [j,i) to j;
             predecessor = j;

    }

    /*****************************//**
    * Call to ensure that all the fingers are pointing properly:
    **********************************/
    public void fixFingers() {

        long id= guid;
        try {
            long nextId;
            if (nextFinger == 0)
                nextId = (this.getId() + (1 << nextFinger));
            else
                nextId = finger[nextFinger -1].getId();
            finger[nextFinger] = locateSuccessor(nextId);

            if (finger[nextFinger].getId() == guid)
                finger[nextFinger] = null;
            else
                nextFinger = (nextFinger + 1) % M;
        }
        catch(RemoteException | NullPointerException e){
            finger[nextFinger] = null;
            e.printStackTrace();
        }
    }

    /*****************************//**
    * Call to ensure that current Chords predecessor is alive and not null:
    **********************************/
    public void checkPredecessor() {
      try {
          if (predecessor != null && !predecessor.isAlive())
              predecessor = null;
      }
      catch(RemoteException e)
      {
          predecessor = null;
//           e.printStackTrace();
      }
    }

    /*****************************//**
    * Constructor for Chord passing in a port and the GUID:
    * \param port Port of Chord
    * \param giud Desired GUID of Chord
    **********************************/
    public Chord(int port, long guid) throws RemoteException {
        int j;
	    finger = new ChordMessageInterface[M];
        for (j=0;j<M; j++){
	       finger[j] = null;
     	}
        this.guid = guid;

        predecessor = null;
        successor = this;
        Timer timer = new Timer();
        timer.scheduleAtFixedRate(new TimerTask() {
	    @Override
	    public void run() {
            stabilize();
            fixFingers();
            checkPredecessor();
            }
        }, 500, 500);
        try{
            // create the registry and bind the name and object.
            System.out.println(guid + " is starting RMI at port="+port);
            registry = LocateRegistry.createRegistry( port );
            registry.rebind("Chord", this);
        }
        catch(RemoteException e){
	       throw e;
        }
    }

    /*****************************//**
    * Print out in console a users predecessor,successor, and fingers:
    **********************************/
    void Print()
    {
        int i;
        try {
            if (successor != null)
                System.out.println("successor "+ successor.getId());
            if (predecessor != null)
                System.out.println("predecessor "+ predecessor.getId());
            for (i=0; i<M; i++)
            {
                try {
                    if (finger != null)
                        System.out.println("Finger "+ i + " " + finger[i].getId());
                } catch(NullPointerException e)
                {
                    finger[i] = null;
                }
            }
        }
        catch(RemoteException e){
	       System.out.println("Cannot retrive id");
        }
    }


}
