package heap;
import chainexception.ChainException;
import global.*;

/**
 * Created by Shubhika on 2/20/2017.
 */
public class HeapScan {
    heap.HeapFile hf;
    HFPage directory;
    HFPage current;
    HFPage currentPage;
    private boolean init;
    RID rid;
    RID Drid;
    boolean free;

    protected HeapScan(heap.HeapFile hf){
        init = true;
        this.hf = hf;
        this.directory = hf.directory;
        this.current = hf.directory;
        global.Minibase.BufferManager.pinPage(current.getCurPage(), current, false);
        //System.out.println("pin");
        RID id = current.firstRecord();
        byte [] pageid = current.selectRecord(id);
        PageId page1 = new PageId(global.Convert.getIntValue(0, pageid));
        currentPage = new HFPage();
        global.Minibase.BufferManager.pinPage(page1, currentPage, false);
        //System.out.println("pin");
        this.rid = currentPage.firstRecord();
        this.Drid = current.firstRecord();
    }
    protected void finalize() throws Throwable{
        super.finalize();
        close();
        current = null;
        currentPage = null;
        hf = null;
    }
    public void close() throws ChainException{
        //System.out.println("HERERERERERERRERE");
        if(!free) {
            global.Minibase.BufferManager.unpinPage(current.getCurPage(), false);
            //System.out.println("unpin");
            global.Minibase.BufferManager.unpinPage(currentPage.getCurPage(), false);
            //System.out.println("unpin");
            free = true;
        }
    }
    public boolean hasNext(){
        if(Drid != null || (current.getNextPage().pid != 0 && current.getNextPage().pid != -1)){
            return true;
        }
        return false;
    }
    public heap.Tuple getNext(RID rid) throws ChainException{
        if (this.rid == null) {
            if (hasNext()) {
                RID space = null;
                if(Drid.pageno.pid != -1) {
                    space = current.nextRecord(Drid);
                }
                RID curr = null;
                if (space != null){
                    curr = current.nextRecord(space);
                }
                if (curr == null) {
                    PageId temp = current.getNextPage();
                    if(temp.pid == -1){
                        close();
                        free = true;
                        return null;
                    }
                    //System.out.println("herrree!!!");
                    //System.out.println(current.getNextPage());
                    Minibase.BufferManager.unpinPage(current.getCurPage(),false);
                    Minibase.BufferManager.pinPage(temp, current, false);
                    Drid.copyRID(current.firstRecord());
                    byte[] pageid = current.selectRecord(Drid);
                    PageId page1 = new PageId(global.Convert.getIntValue(0, pageid));
                    Minibase.BufferManager.unpinPage(currentPage.getCurPage(), false);
                    currentPage = new HFPage();
                    global.Minibase.BufferManager.pinPage(page1, currentPage, false);
                    this.rid = currentPage.firstRecord();
                }else {
                    byte[] pageid = current.selectRecord(curr);
                    PageId page1 = new PageId(global.Convert.getIntValue(0, pageid));
                    Minibase.BufferManager.unpinPage(currentPage.getCurPage(), false);
                    currentPage = new HFPage();
                    global.Minibase.BufferManager.pinPage(page1, currentPage, false);
                    this.rid = currentPage.firstRecord();
                    Drid.copyRID(curr);
                }
            }
        }
        if (this.rid != null) {
            rid.copyRID(this.rid);
            this.rid = currentPage.nextRecord(rid);
            return new heap.Tuple(currentPage.selectRecord(rid));
        }
        return null;
    }
}
