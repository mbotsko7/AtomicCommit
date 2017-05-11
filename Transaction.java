import java.io.Serializable;
/**
 * Created by bardsko on 5/9/17.
 */
public class Transaction implements Serializable{
    public enum Operation { WRITE, DELETE}
    public enum Vote {YES, NO}
    private long transactionId;
    private long guid;
    FileStream fileStream;
    private Operation op; //no transaction without operation
    private Vote v; //for getDecision in Chord
    private long coordinator;

    public Transaction(long coord, long GUID, FileStream fs, Operation ops){
        guid = GUID;
        fileStream = fs;
        transactionId = System.currentTimeMillis();
        op = ops;
        coordinator = coord;
    }

    public void setVote(boolean choice){
        if(choice)
            v = Vote.YES;
        else
            v = Vote.NO;
    }

    public boolean getVote(){
        if(v == Vote.YES)
            return true;
        return false;
    }

    public Operation getOp(){
        return op;
    }

    public FileStream getFileStream() {
        return fileStream;
    }

    public long getTransactionId() {
        return transactionId;
    }

    public long getGuid() {
        return guid;
    }

    public long getCoordinator(){
        return coordinator;
    }
}
