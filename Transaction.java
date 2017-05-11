/**
 * Created by bardsko on 5/9/17.
 */
public class Transaction {
    public enum Operation { WRITE, DELETE}
    public enum Vote {YES, NO}
    private Integer transactionId;
    private long guid;
    FileStream fileStream;
    private Operation op; //no transaction without operation
    private Vote v; //for getDecision in Chord
    private long coordinator;

    public Transaction(long coord, long GUID, FileStream fs, int ID, Operation ops){
        guid = GUID;
        fileStream = fs;
        transactionId = ID;
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

    public Integer getTransactionId() {
        return transactionId;
    }

    public long getGuid() {
        return guid;
    }

    public long getCoordinator(){
        return coordinator;
    }
}
