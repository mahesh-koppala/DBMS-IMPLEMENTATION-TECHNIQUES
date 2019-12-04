import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//import org.apache.hadoop.mapreduce.lib.input.FileSplit;


public class IMDB {

  public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text term = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      try {
    	  String movie = "movie";
          StringTokenizer iteration = new StringTokenizer(value.toString(),"\n");
          while (iteration.hasMoreTokens()) {
        	String[] arr = iteration.nextToken().split(";");
        	String genre = arr[4];
        	String year = arr[3];
        	String name = arr[2];
        	List<String> genreList = Arrays.asList(genre.split(","));
        	if(movie.equalsIgnoreCase(arr[1]) && Integer.parseInt(year)>=2001 && Integer.parseInt(year)<=2015){
                if(genreList.contains("Comedy") && genreList.contains("Romance") && (Integer.parseInt(year)>=2001 || Integer.parseInt(year)<=2005)) {
                	term.set("Comedy,Romance;"+"2001-2005");
                    context.write(term, one);
                }
                else if(genreList.contains("Comedy") && genreList.contains("Romance") && (Integer.parseInt(year)>=2006 || Integer.parseInt(year)<=2010)) {
                	term.set("Comedy,Romance;"+"2006-2010");
                	context.write(term, one);
                }
                else if(genreList.contains("Comedy") && genreList.contains("Romance") && (Integer.parseInt(year)>=2011 || Integer.parseInt(year)<=2015)) {
                	term.set("Comedy,Romance;"+"2011-2015");
                	context.write(term, one);
                }
                if(genreList.contains("Action") && genreList.contains("Thriller") && (Integer.parseInt(year)>=2001 || Integer.parseInt(year)<=2005)) {
                	term.set("Action,Thriller;"+"2001-2005");
                	context.write(term, one);
                }
                else if(genreList.contains("Action") && genreList.contains("Thriller") && (Integer.parseInt(year)>=2006 || Integer.parseInt(year)<=2010)) {
                	term.set("Action,Thriller;"+"2006-2010");
                	context.write(term, one);
                }
                else if(genreList.contains("Action") && genreList.contains("Thriller") && (Integer.parseInt(year)>=2011 || Integer.parseInt(year)<=2015)) {
                	term.set("Action,Thriller;"+"2011-2015");
                	context.write(term, one);
                }
                if(genreList.contains("Adventure") && genreList.contains("Sci-Fi") && (Integer.parseInt(year)>=2001 || Integer.parseInt(year)<=2005)) {
                	term.set("Adventure,Sci-Fi;"+"2001-2005");
                	context.write(term, one);
                }
                else if(genreList.contains("Adventure") && genreList.contains("Sci-Fi") && (Integer.parseInt(year)>=2006 || Integer.parseInt(year)<=2010)) {
                	term.set("Adventure,Sci-Fi;"+"2006-2010");
                	context.write(term, one);
                }
                else if(genreList.contains("Adventure") && genreList.contains("Sci-Fi") && (Integer.parseInt(year)>=2011 || Integer.parseInt(year)<=2015)) {
                	term.set("Adventure,Sci-Fi;"+"2011-2015");
                	context.write(term, one);
                }
        	}
          }
        } catch(Exception e) {
        	e.printStackTrace();
        }
      }
      
  }

  public static class IntSumReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
    
    private IntWritable resultset = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
    
        int total = 0;
        for (IntWritable value : values) {
          total += value.get();
        }
        resultset.set(total);
        context.write(key, resultset);
    
      
    }
  }

  public static void main(String[] args) throws Exception {
	
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf,"IMDB JOB");
    job.setJarByClass(IMDB.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
