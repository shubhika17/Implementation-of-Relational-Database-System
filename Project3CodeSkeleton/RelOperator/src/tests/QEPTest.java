package tests;

// YOUR CODE FOR PART3 SHOULD GO HERE.
import global.AttrOperator;
import global.AttrType;
import global.RID;
import global.SearchKey;
import heap.HeapFile;
import index.HashIndex;
import relop.FileScan;
import relop.SortMergeJoin;
import relop.IndexScan;
import relop.KeyScan;
import relop.Predicate;
import relop.Projection;
import relop.Schema;
import relop.Selection;
import relop.SimpleJoin;
import relop.Tuple;
import java.io.*;

public class QEPTest extends TestDriver {

    /** Department table schema. */
    private static Schema s_dept;

    /** Employee table schema. */
    private static Schema s_emp;

    private static HeapFile deptFile;
    private static HeapFile empFile;

    public static void main(String argv[]) {
        //ONE ARG THAT POINTS TO FILES DEPT AND EMP
        String path = argv[0];

        // initialize schema for the "Department" table
        //DeptId, Name, MinSalary, MaxSalary
        s_dept = new Schema(4);
        s_dept.initField(0, AttrType.INTEGER, 4, "DeptId");
        s_dept.initField(1, AttrType.STRING, 30, "Name");
        s_dept.initField(2, AttrType.INTEGER, 4, "MinSalary");
        s_dept.initField(3, AttrType.INTEGER, 4, "MaxSalary");

        // initialize schema for the "Employee" table
        // EmpId, Name, Age, Salary, DeptID
        s_emp = new Schema(5);
        s_emp.initField(0, AttrType.INTEGER, 4, "EmpId");
        s_emp.initField(1, AttrType.STRING, 20, "Name");
        s_emp.initField(2, AttrType.INTEGER, 4, "Age");
        s_emp.initField(3, AttrType.INTEGER, 4, "Salary");
        s_emp.initField(4, AttrType.INTEGER, 4, "DeptId");

        //initCounts();
        //saveCounts(null);

        // create a clean Minibase instance
        QEPTest qep = new QEPTest();
        qep.create_minibase();

        File folder = new File(path);
        File[] listOfFiles = folder.listFiles();

        for (int i = 0; i < listOfFiles.length; i++) {
            if (listOfFiles[i].isFile()) {
                String fullPath = path + "/" + listOfFiles[i].getName();
                if (listOfFiles[i].getName().equals("Department.txt")) {
                    FileReader fr = null;
                    BufferedReader br = null;
                    try {
                        fr = new FileReader(fullPath);
                        br = new BufferedReader(fr);
                        String line;
                        br.readLine();	//skips schema
                        // create and populate a temporary Department file and index
                        Tuple tuple = new Tuple(s_dept);
                        deptFile = new HeapFile(null);
                        //HashIndex deptIndex = new HashIndex(null);	//DON'T KNOW IF I NEED AN INDEX
                        while ((line = br.readLine()) != null) {
                            //loop through Department file 1 line at a time
                            //to read in the data, and create the tuple
                            String[] fieldArray = line.split(",");
                            tuple.setField(0, Integer.parseInt(fieldArray[0].replaceAll("\\s","")));
                            tuple.setField(1, fieldArray[1].replaceAll("\\s",""));
                            tuple.setField(2, Integer.parseInt(fieldArray[2].replaceAll("\\s","")));
                            tuple.setField(3, Integer.parseInt(fieldArray[3].replaceAll("\\s","")));

                            // insert the tuple in the file and index
                            RID rid = deptFile.insertRecord(tuple.getData());
                            //deptIndex.insertEntry(new SearchKey(age), rid);
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    } finally {
                        try {
                            if (br != null)
                                br.close();
                            if (fr != null)
                                fr.close();
                        } catch (Exception ex) {
                            ex.printStackTrace();
                        }
                    }
                    //saveCounts("insert");
                }

                if (listOfFiles[i].getName().equals("Employee.txt")) {
                    FileReader fr = null;
                    BufferedReader br = null;
                    try {
                        fr = new FileReader(fullPath);
                        br = new BufferedReader(fr);
                        String line;
                        br.readLine();	//skips schema
                        // create and populate a temporary Department file and index
                        Tuple tuple = new Tuple(s_emp);
                        empFile = new HeapFile(null);
                        //HashIndex empIndex = new HashIndex(null);	//DON'T KNOW IF I NEED AN INDEX
                        while ((line = br.readLine()) != null) {
                            //loop through Department file 1 line at a time
                            //to read in the data, and create the tuple
                            String[] fieldArray = line.split(",");
                            tuple.setField(0, Integer.parseInt(fieldArray[0].replaceAll("\\s","")));
                            tuple.setField(1, fieldArray[1].replaceAll("\\s",""));
                            tuple.setField(2, Integer.parseInt(fieldArray[2].replaceAll("\\s","")));
                            tuple.setField(3, Integer.parseInt(fieldArray[3].replaceAll("\\s","")));
                            tuple.setField(4, Integer.parseInt(fieldArray[4].replaceAll("\\s","")));

                            // insert the tuple in the file and index
                            RID rid = empFile.insertRecord(tuple.getData());
                            //empIndex.insertEntry(new SearchKey(age), rid);
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    } finally {
                        try {
                            if (br != null)
                                br.close();
                            if (fr != null)
                                fr.close();
                        } catch (Exception ex) {
                            ex.printStackTrace();
                        }
                    }
                }
                //saveCounts("insert");		//DON'T KNOW WHAT SAVECOUNTS DOES!!!!!!!!!!
            }
        }

        // run all the test cases
        System.out.println("\n" + "Running QEP TESTS ...");
        boolean status = PASS;
        status &= qep.test1();
        status &= qep.test2();
        status &= qep.test3();
        status &= qep.test4();

        // display the final results
        System.out.println();
        if (status != PASS) {
            System.out.println("Error(s) encountered during QEP TESTS.");
        } else {
            System.out.println("All QEP TESTS completed; verify output for correctness.");
        }

    } // public static void main (String argv[])

    protected boolean test1() {		//1. Display for each employee their ID, Name and age
        try {

            System.out.println("\nTest 1: Display for each employee their ID, Name and age");
            //saveCounts(null);
            FileScan scan = new FileScan(s_emp, empFile);
            Projection pro = new Projection(scan, 0, 1, 2);
            pro.execute();
            //saveCounts("project");

            // destroy temp files before doing final counts
            pro = null;
            scan = null;
            System.gc();
            //saveCounts("finish1");

            // that's all folks!
            System.out.print("\n\nTest 1 completed without exception.");
            return PASS;

        } catch (Exception exc) {

            exc.printStackTrace(System.out);
            System.out.print("\n\nTest 1 terminated because of exception.");
            return FAIL;

        } finally {
            //printSummary(2);
            System.out.println();
        }
    } // protected boolean test1()

    protected boolean test2() {		//2. Display the Name for the departments with MinSalary = MaxSalary
        try {

            System.out.println("\nTest 2: Display the Name for the departments with MinSalary = MaxSalary");

            // select where minsal = maxsal
            //saveCounts(null);
            Predicate pred = new Predicate(AttrOperator.EQ, AttrType.FIELDNO, 2, AttrType.FIELDNO, 3);
            FileScan scan = new FileScan(s_dept, deptFile);
            Selection sel = new Selection(scan, pred);
            Projection pro = new Projection(sel, 1);
            pro.execute();
            //saveCounts("both");

            // destroy temp files before doing final counts
            sel = null;
            pro = null;
            scan = null;
            System.gc();
            //saveCounts("finish2");

            // that's all folks!
            System.out.print("\n\nTest 2 completed without exception.");
            return PASS;

        } catch (Exception exc) {

            exc.printStackTrace(System.out);
            System.out.print("\n\nTest 2 terminated because of exception.");
            return FAIL;

        } finally {
            //printSummary(2);
            System.out.println();
        }
    } // protected boolean test2()

    protected boolean test3() {		//3. For each employee, display their Name and the Name of their department
        // as well as the minimum salary of their department
        try {

            System.out.println("\nTest 3: For each employee, display their Name and the Name of their department as well as the minimum salary of their department");
            //saveCounts(null);
            SortMergeJoin join = new SortMergeJoin(new FileScan(s_dept, deptFile),
                    new FileScan(s_emp, empFile), 0, 4);
            Projection pro = new Projection(join, 5, 1, 2);
            pro.execute();

            // destroy temp files before doing final counts
            join = null;
            pro = null;
            System.gc();
            //saveCounts("finish3");

            // that's all folks!
            System.out.print("\n\nTest 3 completed without exception.");
            return PASS;

        } catch (Exception exc) {

            exc.printStackTrace(System.out);
            System.out.print("\n\nTest 3 terminated because of exception.");
            return FAIL;

        } finally {
            //printSummary(1);
            System.out.println();
        }
    } // protected boolean test3()

    protected boolean test4() {		//4. Display the Name for each employee whose Salary is less than the maximum salary and
        //greater than the minimum salary of their department.
        try {

            System.out.println("\nTest 4: Display the Name for each employee whose Salary is less than the maximum salary and greater than the minimum salary of their department.");
            //saveCounts(null);
            SortMergeJoin join = new SortMergeJoin(new FileScan(s_dept, deptFile),
                    new FileScan(s_emp, empFile), 0, 4);
            Predicate pred1 = new Predicate(AttrOperator.GT, AttrType.FIELDNO, 7, AttrType.FIELDNO, 2);		//greater than minsal
            Predicate pred2 = new Predicate(AttrOperator.LT, AttrType.FIELDNO, 7, AttrType.FIELDNO,	3);	//less than maxsal
            Selection sel1 = new Selection(join, pred1);
            Selection sel2 = new Selection(sel1, pred2);
            Projection pro = new Projection(sel2, 5);
            pro.execute();

            // destroy temp files before doing final counts
            join = null;
            pred1 = null;
            pred2 = null;
            sel1 = null;
            sel2 = null;
            pro = null;
            System.gc();
            //saveCounts("finish3");

            // that's all folks!
            System.out.print("\n\nTest 4 completed without exception.");
            return PASS;

        } catch (Exception exc) {

            exc.printStackTrace(System.out);
            System.out.print("\n\nTest 4 terminated because of exception.");
            return FAIL;

        } finally {
            //printSummary(1);
            System.out.println();
        }
    } // protected boolean test4()

}
