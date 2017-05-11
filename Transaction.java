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

    public Transaction(long GUID, FileStream fs, int ID, Operation ops){
        guid = GUID;
        fileStream = fs;
        transactionId = ID;
        op = ops;
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
}
