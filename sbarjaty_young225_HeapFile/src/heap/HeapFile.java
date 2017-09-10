package heap;
import chainexception.ChainException;
import global.*;

import java.io.IOException;


/**
 * Created by Shubhika on 2/20/2017.
 */

public class HeapFile implements GlobalConst {
    HFPage directory;
    int numberOfRecords = 0;
    HFPage current;
    HFPage currentPage;
    PageId directoryPID;

    public HeapFile(String name) throws IOException {
        PageId page1;
        //System.out.println();
        HFPage page = new HFPage();
        if(name == null){
            page1 = global.Minibase.BufferManager.newPage(page, 1);
            //System.out.println("pin");
            int freeSpace = page.getFreeSpace();
            page.setCurPage(page1);
            currentPage = page;
            global.Minibase.BufferManager.unpinPage(page1, true);
            //System.out.println("unpin");
            directory = new HFPage();
            directoryPID = Minibase.BufferManager.newPage(directory, 1);
            //System.out.println("pin");
            directory.setCurPage(directoryPID);
            byte [] data = new byte[100];
            global.Convert.setIntValue(page1.pid, 0, data);
            directory.insertRecord(data);
            global.Convert.setIntValue(freeSpace, 0, data);
            directory.insertRecord(data);
            //System.out.println("incon: " + directory.getCurPage());
            current = directory;
            global.Minibase.BufferManager.unpinPage(directoryPID, true);
            //System.out.println("unpin");
            return;
        }
        page1 = global.Minibase.DiskManager.get_file_entry(name);
        if (page1== null){
            page1 = global.Minibase.BufferManager.newPage(page, 1);
            //System.out.println("pin");
            global.Minibase.DiskManager.add_file_entry(name, page1);
            page.setCurPage(page1);
            currentPage = page;
            int freeSpace = page.getFreeSpace();
            global.Minibase.BufferManager.unpinPage(page1, true);
            //System.out.println("unpin");
            directory = new HFPage();
            directoryPID = global.Minibase.BufferManager.newPage(directory,1);
            //System.out.println("pin");
            directory.setCurPage(directoryPID);
            byte [] data = new byte[100];
            global.Convert.setIntValue(page1.pid, 0, data);
            directory.insertRecord(data);
            global.Convert.setIntValue(freeSpace, 0, data);
            directory.insertRecord(data);
            current = directory;
            //System.out.println("incon: " + directoryPID);
            global.Minibase.BufferManager.unpinPage(directoryPID, true);
            //System.out.println("unpin");
            return;
        }
        global.Minibase.BufferManager.pinPage(page1, page, false);
        int freeSpace = page.getFreeSpace();
        page.setCurPage(page1);
        currentPage = page;
        global.Minibase.BufferManager.unpinPage(page1, true);
        directory = new HFPage();
        directoryPID = Minibase.BufferManager.newPage(directory, 1);
        directory.setCurPage(directoryPID);
        byte [] data = new byte[100];
        global.Convert.setIntValue(page1.pid, 0, data);
        directory.insertRecord(data);
        global.Convert.setIntValue(freeSpace, 0, data);
        directory.insertRecord(data);
        current = directory;
        global.Minibase.BufferManager.unpinPage(directoryPID, true);
        global.Minibase.BufferManager.pinPage(page1, currentPage, false);
        RID temp = page.firstRecord();
        while (temp != null) {
            temp = page.nextRecord(temp);
            numberOfRecords++;
        }
        global.Minibase.BufferManager.unpinPage(page1, false);
    }
    public RID insertRecord(byte[] record) throws IllegalArgumentException,
            ChainException, IOException {
        if (record.length > GlobalConst.MAX_TUPSIZE)
            throw new SpaceNotAvailableException("Length too big");
        RID crid = directory.firstRecord();
        HFPage currDirectory = directory;
        while(crid != null) {
            //System.out.println(currDirectory.getCurPage());
            global.Minibase.BufferManager.pinPage(currDirectory.getCurPage(), currDirectory, false);
            byte[] tuple = currDirectory.selectRecord(crid);
            int pid = global.Convert.getIntValue(0, tuple);
            RID space = currDirectory.nextRecord(crid);
            //System.out.println(space);
            byte [] freeSpace = currDirectory.selectRecord(space);
            int intFreeSpace = global.Convert.getIntValue(0, freeSpace);
            if (intFreeSpace > record.length) {
                PageId id = new PageId(pid);
                HFPage page = new HFPage();
                global.Minibase.BufferManager.pinPage(id, page, false);
                RID rid = page.insertRecord(record);
                int updatedSpace = page.getFreeSpace();
                byte [] data = new byte[100];
                global.Convert.setIntValue(updatedSpace, 0, data);
                heap.Tuple upSpace = new heap.Tuple(data);
                currDirectory.updateRecord(space, upSpace);
                global.Minibase.BufferManager.unpinPage(id, true);
                global.Minibase.BufferManager.unpinPage(currDirectory.getCurPage(), true);
                numberOfRecords++;
                return rid;
            }
            crid = currDirectory.nextRecord(space);
            if(crid == null){
                if(currDirectory.getNextPage().pid != 0 && currDirectory.getNextPage().pid != -1 ) {
                    global.Minibase.BufferManager.unpinPage(currDirectory.getCurPage(), true);
                    PageId pageID = currDirectory.getNextPage();
                    currDirectory = new HFPage();
                    global.Minibase.BufferManager.pinPage(pageID, currDirectory, false);
                    crid = currDirectory.firstRecord();
                }
            }
            global.Minibase.BufferManager.unpinPage(currDirectory.getCurPage(), false);
        }
        HFPage page = new HFPage();
        PageId id = global.Minibase.BufferManager.newPage(page, 1);
        global.Minibase.BufferManager.pinPage(currDirectory.getCurPage(), currDirectory, false);
        page.setCurPage(id);
        RID rid = page.insertRecord(record);
        int freeSpace = page.getFreeSpace();
        byte [] data = new byte[100];
        global.Convert.setIntValue(id.pid, 0, data);
        RID curr = currDirectory.insertRecord(data);
        byte [] space = new byte[100];
        global.Convert.setIntValue(freeSpace, 0, space);
        RID currSpace = currDirectory.insertRecord(data);
        if(curr == null || currSpace == null){
            if(curr != null){
                currDirectory.deleteRecord(curr);
            }
            if(currSpace != null){
                currDirectory.deleteRecord(curr);
            }
            //System.out.println("here");
            HFPage directoryPage = new HFPage();
            PageId directoryPid = global.Minibase.BufferManager.newPage(directoryPage, 1);
            directoryPage.setCurPage(directoryPid);
            //System.out.println(directoryPage.getCurPage());
            currDirectory.setNextPage(directoryPid);
            directoryPage.setPrevPage(currDirectory.getCurPage());
            global.Minibase.BufferManager.unpinPage(currDirectory.getCurPage(), true);
            global.Minibase.BufferManager.unpinPage(directoryPid, true);
            currDirectory = directoryPage;
            global.Minibase.BufferManager.pinPage(currDirectory.getCurPage(), currDirectory, false);
            currDirectory.insertRecord(data);
            currDirectory.insertRecord(space);
        }
        global.Minibase.BufferManager.unpinPage(id, true);
        Minibase.BufferManager.unpinPage(currDirectory.getCurPage(),true);
        numberOfRecords++;
        return rid;
    }
    public heap.Tuple getRecord(RID rid) {
        PageId pid = rid.pageno;
        if(pid.pid == 0 || pid.pid == -1)
            throw new IllegalStateException();
        HFPage page = new HFPage();
        global.Minibase.BufferManager.pinPage(pid, page, false);
        //System.out.println("pin");
        byte [] data = page.selectRecord(rid);
        Minibase.BufferManager.unpinPage(pid, false);
        //System.out.println("unpin");
        return (new heap.Tuple(data));
    }
    public boolean updateRecord(RID rid, heap.Tuple newRecord) throws ChainException{
        PageId pid = rid.pageno;
        HFPage page = new HFPage();
        global.Minibase.BufferManager.pinPage(pid, page, false);
        //System.out.println("pin");
        if (newRecord.getLength() != page.selectRecord(rid).length) {
            global.Minibase.BufferManager.unpinPage(pid, false);
            //System.out.println("unpin");
            throw new InvalidUpdateException();
        }
        page.updateRecord(rid, newRecord);
        global.Minibase.BufferManager.unpinPage(rid.pageno, true);
        //System.out.println("unpin");
        return true;
    }
    public boolean deleteRecord(RID rid){
        PageId pid = rid.pageno;
        if(pid.pid == 0 || pid.pid == -1) {
            return false;
        }
        HFPage page = new HFPage();
        global.Minibase.BufferManager.pinPage(pid, page, false);
        //System.out.println("pin");
        page.deleteRecord(rid);
        int freeSpace = page.getFreeSpace();
        global.Minibase.BufferManager.unpinPage(pid, true);
        //System.out.println("unpin");
        RID crid = directory.firstRecord();
        HFPage currDirectory = directory;
        while(crid != null) {
            //System.out.println(currDirectory.getCurPage());
            global.Minibase.BufferManager.pinPage(currDirectory.getCurPage(), currDirectory, false);
            byte[] tuple = currDirectory.selectRecord(crid);
            int id = global.Convert.getIntValue(0, tuple);
            RID space = currDirectory.nextRecord(crid);
            if (id == rid.pageno.pid) {
                int updatedSpace = page.getFreeSpace();
                byte [] data = new byte[100];
                global.Convert.setIntValue(updatedSpace, 0, data);
                heap.Tuple upSpace = new heap.Tuple(data);
                currDirectory.updateRecord(space, upSpace);
                global.Minibase.BufferManager.unpinPage(currDirectory.getCurPage(), true);
                break;
            }
            crid = currDirectory.nextRecord(space);
            if(crid == null){
                if(currDirectory.getNextPage().pid != 0 && currDirectory.getNextPage().pid != -1 ) {
                    global.Minibase.BufferManager.unpinPage(currDirectory.getCurPage(), true);
                    PageId pageID = currDirectory.getNextPage();
                    currDirectory = new HFPage();
                    global.Minibase.BufferManager.pinPage(pageID, currDirectory, false);
                    crid = currDirectory.firstRecord();
                }
            }
            global.Minibase.BufferManager.unpinPage(currDirectory.getCurPage(), false);
        }
        return true;
    }
    public int getRecCnt(){
        return numberOfRecords;
    }
    public heap.HeapScan openScan(){
        return new heap.HeapScan(this);
    }
}
