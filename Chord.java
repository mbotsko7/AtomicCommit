import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.math.BigInteger;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Timer;
import java.util.TimerTask;

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
    
    String filePath, tmpPath;

    /*****************************//**
    * Validate a transaction:
    * \param time IP of Chord you are looking for
    **********************************/
    public boolean transactionValid(long time, long filestime, long g){
        boolean valid = true;
        for(File f:new File(tmpPath).listFiles()){
            Transaction t;
            try{
                InputStream ostream = new FileInputStream(f);
                ObjectInputStream tstream = new ObjectInputStream(ostream);
                t = (Transaction)tstream.readObject();

                if(t.getTransactionId() > time)
                    valid = false;

            }
            catch (Exception e){
                e.printStackTrace();
            }


        }
        for(File f:new File(filePath+g).listFiles()){
            if(f.lastModified() > filestime)
                valid = false;
        }
        return valid;
    }
    
    /*****************************//**
    * Node checks whether it can commit:
    * \param trans A transaction from the coordinator
    **********************************/
    public boolean canCommit(Transaction trans){
        File pathtofile = new File(filePath+trans.getGuid());
        long mytime = pathtofile.lastModified();
        if(transactionValid(trans.getTransactionId(), mytime, trans.getGuid())){
            trans.setVote(true);
            File f = new File(tmpPath+trans.getTransactionId());
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

    /*****************************//**
    * Instruct participant to perform the file write:
    * \param trans Transaction from coordinator
    **********************************/
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

    /*****************************//**
    * Coordinator tells participants to abort their commit:
    * \param trans Transaction that is from coordinator
    **********************************/
    public void doAbort(Transaction trans){
        File f = new File(tmpPath+trans.getTransactionId());
        if(f.exists()) //in case this participant said no
            f.delete();
    }

    /*****************************//**
    * Reponse from participant to coordinator stating you have commited:
    * \param trans Transaction sent to coordinator
    * \param participant The guid of the current node that's acting as the participant
    **********************************/
    public boolean haveCommitted(Transaction trans, long participant){
        File f = new File(filePath+trans.getGuid());
        if(f.exists())
            return true;
        return false;
    }

    /*****************************//**
    * Request from participant asking what vote was for missing response from coordinator:
    * \param trans Transaction
    **********************************/
    public boolean getDecision(Transaction trans){
        try {
            File f = new File(tmpPath+trans.getTransactionId());
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

    /*****************************//**
    * Write a specified file given a file name:
    * \param fileName The name of the file to be written
    **********************************/
    public void writeFile(String fileName){
      String path = "./"+  this.guid +"/"+fileName;
        try{
            boolean vote = true;
            //tally votes
            for(int i = 0; i < 3; i++){
                long id = md5(fileName+i+1);
                ChordMessageInterface c = locateSuccessor(id);
                Transaction t = new Transaction( guid, id, new FileStream(path), Transaction.Operation.WRITE);
                if(c.canCommit(t) == false)
                    vote = false;
            }
            //if yes
            if(vote){
                for(int i = 0; i < 3; i++){
                    long id = md5(fileName+i+1);
                    ChordMessageInterface c = locateSuccessor(id);
                    c.doCommit(new Transaction(guid, id, new FileStream(path), Transaction.Operation.WRITE));

                }
            }
            else{ //if no
              System.out.println("Failure, please read");
                for(int i = 0; i < 3; i++){
                    long id = md5(fileName+i+1);
                    ChordMessageInterface c = locateSuccessor(id);
                    c.doAbort(new Transaction(guid, id, new FileStream(path), Transaction.Operation.WRITE));

                }
            }


        }catch(IOException e){
            e.printStackTrace();
        }
    }

    /*****************************//**
    * Write an entire directory to the reposity:
    * \param directoryName The path of the directory
    **********************************/
    public void writeDirectory(String directoryName){
    	String path = "./"+  this.guid +"/"+directoryName;
        File f = new File(path);
        for(File file : f.listFiles()){
        	String filePath = file.getPath().substring(file.getPath().indexOf("/", 2)+1);
            writeFile(filePath);
        }
    }

    /*****************************//**
    * Read a file from the repository and save it:
    * \param fileName
    **********************************/
    public void readFile(String fileName){
        String path = "";
        ChordMessageInterface peer;
        InputStream s = null;
        try{
            for(int i=0;i<3;i++){
              long guidObject = md5(fileName+i+1);
              path = "./"+this.guid+"/"+fileName;
              peer = locateSuccessor(guidObject);
              s = peer.get(guidObject);
              if(s != null){
                break;
              }
            }
            FileOutputStream output = new FileOutputStream(path);
            try{
              while (s.available() > 0){
                  output.write(s.read());
              }
              output.close();
            }catch(Exception e){}
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
                String path = "./"+c.getId()+"/repository/"+id;
                Transaction t = new Transaction( guid, id, new FileStream(path), Transaction.Operation.DELETE);
                if(c.canCommit(t) == false)
                    vote = false;
            }
            //if yes
            if(vote){
                for(int i = 0; i < 3; i++){
                    long id = md5(fileName+i+1);
                    ChordMessageInterface c = locateSuccessor(id);
                    String path = "./"+c.getId()+"/repository/"+id;
                    c.doCommit(new Transaction(guid, id, new FileStream(path), Transaction.Operation.DELETE));
                }
            }
            else{ //if no
                for(int i = 0; i < 3; i++){
                    long id = md5(fileName+i+1);
                    ChordMessageInterface c = locateSuccessor(id);
                    String path = "./"+c.getId()+"/repository/"+id;
                    c.doAbort(new Transaction(guid, id, new FileStream(path), Transaction.Operation.DELETE));
                }
            }
        }catch(IOException e){
            e.printStackTrace();
        }

    }

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
          FileOutputStream output = new FileOutputStream(filePath + guidObject);
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
         file = new FileStream(filePath + guidObject);
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
            File fileName =  new File(filePath + guidObject);

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
        return predecessor; //return successor;

    }


    /*****************************//**
    * Leave the Chord ring by passing all files in repository and breaking connections:
    **********************************/
    public void leaveRing(){
        try{
            successor.notify(predecessor);

            //fixFingers();
            stabilize();

            File[] f = new File(filePath).listFiles();
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
        this.guid = guid;
        this.filePath = "./" + guid + "/repository/";
        this.tmpPath = "./" + guid + "/tmp/";
        new File(filePath).mkdirs();
        new File(tmpPath).mkdirs();
	    finger = new ChordMessageInterface[M];
        for (int j=0;j<M; j++){
	       finger[j] = null;
     	}

        predecessor = null;
        successor = this;
        Timer timer = new Timer();
        timer.scheduleAtFixedRate(new TimerTask() {
	    @Override
	    public void run() {
            stabilize();
            //fixFingers();
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
