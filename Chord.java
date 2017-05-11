import javax.swing.tree.TreeNode;
import java.rmi.*;
import java.rmi.registry.*;
import java.rmi.server.*;
import java.net.*;
import java.util.*;
import java.io.*;

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
    public void doCommit(Transaction trans){
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
    }

    public void doAbort(Transaction trans){
        //get rid of transaction?
    }

    public void haveCommitted(Transaction trans, Object participant){

    }

    public boolean getDecision(Transaction trans){
        return trans.getVote();
    }






    /***************************************
     * End Atomic Commit
     */



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
