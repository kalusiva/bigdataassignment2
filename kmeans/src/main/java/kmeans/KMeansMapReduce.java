package kmeans;



import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class KMeansMapReduce {


/*

    public static class Customer implements Serializable {
        private static boolean normalize = false;
        private String customerId;
        private String gender;
        private double age;
        private double annualIncome;
        private double spendingScore;

        private  static double maxAge=0;
        private static double maxAnnualIncome=0;
        private static double maxSpendingScore=0;
        private  static double minAge=0;
        private static double minAnnualIncome=0;
        private static double minSpendingScore=0;

        static {
            System.out.println("static Normalization sgtarted ...");
            BufferedReader br = null;
            ArrayList<Customer> results = new ArrayList<Customer>();
            try {
                Path pt = new Path("hdfs:///user/Mall_Customers.csv");
                FileSystem fs = FileSystem.get(new Configuration());
                 br = new BufferedReader(new InputStreamReader(fs.open(pt)));
                boolean oldNormalize = Customer.normalize;

                int i = 0;
                String line;
                line = br.readLine();
               // System.out.println("line = "+line);
                while ((line = br.readLine()) != null ) {


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
                        results.add(mean);
                    }catch(Exception e) {
                        // ignore
                        System.out.println("static values : ignoring exception "+e);
                    }

                }

                for( i=0;i<results.size();i++) {
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

                try {
                br.close();
                }catch(Exception ee) {
                    System.out.println("static dinally ignoring exception "+ee);
                }
            }
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
    }

    */


    /**
     * This is the main method that takes the Mall Customer.csv as input and performs kmeans based classification.
     * @param args
     * @throws Exception
     */


    public static void main(String[] args) throws Exception {
        int k = 0;
        String outputFilebase = args[1];
        String outputFile = outputFilebase;
        do {
            Configuration conf = new Configuration();
            if(k > 0) {
                conf.set("meansFile",outputFile + "out"+(k-1)+"/part-r-00000");
            }
            Job job = Job.getInstance(conf, "kmeans map reduce");
            job.setJarByClass(KMeansMapReduce.class);
            job.setMapperClass(AssignPointsToClusterMapper.class);
            //job.setCombinerClass(IntSumReducer.class);
            job.setReducerClass(ClusterReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(ObjectWritable.class);


            FileInputFormat.addInputPath(job, new Path(args[0]));
            Path p = new Path(outputFile+k);
            FileSystem fs = FileSystem.get(conf);
            fs.delete(p,true);

            FileOutputFormat.setOutputPath(job, new Path(outputFile+k));
            boolean status = job.waitForCompletion(true);

            if (status) {
                Configuration newmeansconf = new Configuration();
                Job newmeansjob = Job.getInstance(newmeansconf, "kmeans map reduce");
                newmeansjob.setJarByClass(KMeansMapReduce.class);
                newmeansjob.setMapperClass(CalculateMeanMapper.class);
                //newmeansjob.setCombinerClass(CalculateMeanReducer.class);
                newmeansjob.setReducerClass(CalculateMeanReducer.class);
                newmeansjob.setOutputKeyClass(Text.class);
                newmeansjob.setOutputValueClass(Text.class);
                newmeansjob.setMapOutputKeyClass(Text.class);
                newmeansjob.setMapOutputValueClass(Text.class);
                FileInputFormat.addInputPath(newmeansjob, new Path(outputFile+k+"/part-r-00000"));
                Path p1 = new Path(outputFile+ "out"+k);
                FileSystem fs1 = FileSystem.get(conf);
                fs1.delete(p1,true);
                FileOutputFormat.setOutputPath(newmeansjob, new Path(outputFile+ "out"+k));
                status = newmeansjob.waitForCompletion(true);
            }
            k++;

        } while (k < 3);
    }

}