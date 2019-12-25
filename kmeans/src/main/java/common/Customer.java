package common;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.util.ArrayList;

public class Customer implements Serializable, Comparable {
    private static boolean normalize = false;
    private String customerId;
    private String gender;
    private double age;
    private double annualIncome;
    private double spendingScore;
    private int category=999;
    private double distance=-1;

    private  static double maxAge=0;
    private static double maxAnnualIncome=0;
    private static double maxSpendingScore=0;
    private  static double minAge=0;
    private static double minAnnualIncome=0;
    private static double minSpendingScore=0;

    static {
        try {
            ArrayList<Customer> results = getCustomers("hdfs:///user/Mall_Customers.csv");

            boolean oldNormalize = normalize;

            for( int i=0;i<results.size();i++) {
                Customer tmp = results.get(i);
                if(tmp.getAge() > maxAge) {
                    maxAge=tmp.getAge();
                }
                if(tmp.getAnnualIncome() > maxAnnualIncome) {
                    maxAnnualIncome = tmp.getAnnualIncome();
                }
                if(tmp.getSpendingScore() > maxSpendingScore) {
                    maxSpendingScore = tmp.getSpendingScore();
                }
                if(i == 0) {
                    minAge=tmp.getAge();
                    minAnnualIncome=tmp.getAnnualIncome();
                    minSpendingScore=tmp.getSpendingScore();
                }else {
                    if(tmp.getAge() < minAge) {
                        minAge=tmp.getAge();
                    }
                    if(tmp.getAnnualIncome() < minAnnualIncome) {
                        minAnnualIncome = tmp.getAnnualIncome();
                    }
                    if(tmp.getSpendingScore() < minSpendingScore) {
                        minSpendingScore = tmp.getSpendingScore();
                    }
                }
            }

            System.out.println("static block minAge= "+minAge+ " minAnnualIncome = "+minAnnualIncome+" minSpendingScore="+minSpendingScore);
            System.out.println("static block maxAge= "+maxAge+ " maxAnnualIncome = "+maxAnnualIncome+" maxSpendingScore="+maxSpendingScore);

            Customer.normalize = oldNormalize;

        } catch(Exception ee) {
            System.out.println("static block ignoring exception "+ee);
            ee.printStackTrace(System.out);
        }
        finally {
            // you should close out the BufferedReader

            System.out.println("static Normalization ended ...");


        }
    }

    public static ArrayList<Customer> getCustomers(String filePath) {
        System.out.println("static Normalization sgtarted ...");
        BufferedReader br = null;
        ArrayList<Customer> results = new ArrayList<Customer>();
        try {
            Path pt = new Path(filePath);
            FileSystem fs = FileSystem.get(new Configuration());
            br = new BufferedReader(new InputStreamReader(fs.open(pt)));
            boolean oldNormalize = Customer.normalize;

            int i = 0;
            String line;
            line = br.readLine();
            // System.out.println("line = "+line);
            while ((line = br.readLine()) != null) {


                // be sure to read the next line otherwise you'll get an infinite loop

                i++;

                //System.out.println(i+"line = "+line);

                String[] record = line.split(",");

                Customer mean = new Customer();

                Customer.normalize = false;
                try {
                    mean.setCustomerId(record[0]);
                    mean.setGender(record[1]);
                    mean.setAge(Double.parseDouble(record[2]));
                    mean.setAnnualIncome(Double.parseDouble(record[3]));
                    mean.setSpendingScore(Double.parseDouble(record[4]));
                    if (record.length > 5) {
                        mean.setCategory(Integer.parseInt(record[5]));
                    }
                    if (record.length > 6) {
                        mean.setDistance(Double.parseDouble(record[6]));
                    }
                    results.add(mean);
                } catch (Exception e) {
                    // ignore
                    System.out.println("static values : ignoring exception " + e);
                }

            }
        }catch(Exception e) {
            e.printStackTrace(System.out);
        }finally {
            try {
                br.close();
            }catch(Exception ee) {
                System.out.println("static finally ignoring exception "+ee);
            }
        }

        return results;

    }


    public   String toString() {

        String line = getCustomerId()+","+getGender()+","+getAge()+","+getAnnualIncome()+","+getSpendingScore();

        if(getCategory() != 999 ) {
            line+=","+getCategory();
        }

        if(getDistance() > 0 ) {
            line+=","+getDistance();
        }

        return line;

    }

    public  static Customer parseCustomer(String line) {
        String record [] = line.toString().split(",");

        Customer tmp = new Customer();

        try {
            tmp.setCustomerId(record[0]);
            tmp.setGender(record[1]);
            tmp.setAge(Double.parseDouble(record[2]));
            tmp.setAnnualIncome(Double.parseDouble(record[3]));
            tmp.setSpendingScore(Double.parseDouble(record[4]));
            if(record.length > 5) {
                tmp.setCategory(Integer.parseInt(record[5]));
            }
            if(record.length > 6) {
                tmp.setDistance(Double.parseDouble(record[6]));
            }

        }catch(Exception e) {
            e.printStackTrace(System.out);
        }
        return tmp;
    }

    public double distance(Customer target) {

        System.out.println("Data : customerId : "+customerId+" gender : "+gender+" age : "+getAge()+" annualINcome : "+getAnnualIncome()+" spendingScore : "+getSpendingScore());
        System.out.println("Mean : customerId : "+target.getCustomerId()+" gender : "+target.getGender()+" age : "+target.getAge()+" annualINcome : "+target.getAnnualIncome()+" spendingScore : "+target.getSpendingScore());
        int targetGen = 0;
        int sourceGen = 0;

        if(target.getGender().equalsIgnoreCase("male")) {
            targetGen = 1;
        }

        if(getGender().equalsIgnoreCase("male")) {
            sourceGen = 1;
        }


        return Math.sqrt((sourceGen-targetGen)*(sourceGen-targetGen)+
                (target.getAge()-getAge())*(target.getAge()-getAge())+
                (target.getAnnualIncome() - getAnnualIncome())*(target.getAnnualIncome() - getAnnualIncome())+
                (target.getSpendingScore() - getSpendingScore())*(target.getSpendingScore() - getSpendingScore()))
                ;

    }


    public String getCustomerId() {
        return customerId;
    }

    public void setCustomerId(String customerId) {
        this.customerId = customerId;
    }

    public String getGender() {
        return gender;
    }

    public void setGender(String gender) {
        this.gender = gender;
    }

    public double getAge() {
        if(normalize) {
            return ((age-minAge)/(maxAge-minAge));
        }else {
            return age;
        }
    }

    public void setAge(double age) {
        this.age = age;
    }

    public double getAnnualIncome() {
        if(normalize) {
            return (annualIncome - minAnnualIncome)/(maxAnnualIncome-minAnnualIncome);
        }else
            return annualIncome;
    }

    public void setAnnualIncome(double annualIncome) {
        this.annualIncome = annualIncome;
    }

    public double getSpendingScore() {
        if(normalize) {
            return (spendingScore-minSpendingScore)/(maxSpendingScore-minSpendingScore);
        }else
            return spendingScore;
    }

    public void setSpendingScore(double spendingScore) {
        this.spendingScore = spendingScore;
    }

    public void setCategory(int category) {
        this.category = category;
    }

    public int getCategory() {
        return this.category;
    }

    public void setDistance(double distance) {
        this.distance = distance;
    }

    public double getDistance() {
        return distance;
    }

    public int compareTo(Object o) {
        return (int)(getDistance()-((Customer)o).getDistance());
    }
}