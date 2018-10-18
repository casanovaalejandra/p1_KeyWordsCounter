package edu.uprm.cse.bigdata;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import twitter4j.Status;
import twitter4j.TwitterException;
import twitter4j.TwitterObjectFactory;
import java.util.*;
import java.io.IOException;
import java.util.Arrays;

public class KeyWordsMapper extends Mapper<LongWritable, Text,Text, IntWritable> {

//Method to check if a word is part of the set of stop words, returns true if its a stop word, it also checks
    public boolean isStopWord(String wordToCheck, Set<String> list){
        wordToCheck=wordToCheck.toLowerCase();
        if(list.contains(wordToCheck) ){return true;}
        return false;
    }
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException,InterruptedException{
        //ESta clase lo que deber hacer es leer cada tweet y contar dentro del texto cada palabra que si es keyword,
        //es decir que no es un articulo, punto, coma, etc.

        String tweet = value.toString();
        String text = null;
        String[] arraysOfWordText = null;

        //usando twitter4j se convierte el string jason ( el twitter object) a un Status object
        //y con este puedes seleccionar el texto como un field a leer
        //fuente: https://flanaras.wordpress.com/2016/01/11/twitter4j-status-object-string-json/
        Status status = null;
        String id =null;
        Text id2 = null;
        Set<String> stopWOrds =new HashSet<String>(Arrays.asList(new String[] {"a", "about", "above", "above", "across", "after", "afterwards", "again", "against", "all", "almost", "alone",
                "along", "already", "also","although","always","am","among", "amongst", "amoungst", "amount",  "an", "and", "another", "any",
                "anyhow","anyone","anything","anyway", "anywhere", "are", "around", "as",  "at", "back","be","became", "because","become","becomes",
                "becoming", "been", "before", "beforehand", "behind", "being", "below", "beside", "besides", "between", "beyond", "bill", "both",
                "bottom","but", "by", "call", "can", "cannot", "cant", "co", "con", "could", "couldnt", "cry", "de", "describe", "detail", "do",
                "done", "down", "due", "during", "each", "eg", "eight", "either", "eleven","else", "elsewhere", "empty", "enough", "etc", "even",
                "ever", "every", "everyone", "everything", "everywhere", "except", "few", "fifteen", "fify", "fill", "find", "fire", "first", "five",
                "for", "former", "formerly", "forty", "found", "four", "from", "front", "full", "further", "get", "give", "go", "had", "has", "hasnt",
                "have", "he", "hence", "her", "here", "hereafter", "hereby", "herein", "hereupon", "hers", "herself", "him", "himself", "his", "how",
                "however", "hundred", "ie", "if", "in", "inc", "indeed", "interest", "into", "is", "it", "its", "itself", "keep", "last", "latter",
                "latterly", "least", "less", "ltd", "made", "many", "may", "me", "meanwhile", "might", "mill", "mine", "more", "moreover", "most",
                "mostly", "move", "much", "must", "my", "myself", "name", "namely", "neither", "never", "nevertheless", "next", "nine", "no", "nobody",
                "none", "noone", "nor", "not", "nothing", "now", "nowhere", "of", "off", "often", "on", "once", "one", "only", "onto", "or", "other",
                "others", "otherwise", "our", "ours", "ourselves", "out", "over", "own","part", "per", "perhaps", "please", "put", "rather", "re",
                "same", "see", "seem", "seemed", "seeming", "seems", "serious", "several", "she", "should", "show", "side", "since", "sincere",
                "six", "sixty", "so", "some", "somehow", "someone", "something", "sometime", "sometimes", "somewhere", "still", "such", "system",
                "take", "ten", "than", "that", "the", "their", "them", "themselves", "then", "thence", "there", "thereafter", "thereby", "therefore",
                "therein", "thereupon", "these", "they", "thickv", "thin", "third", "this", "those", "though", "three", "through", "throughout", "thru",
                "thus", "to", "together", "too", "top", "toward", "towards", "twelve", "twenty", "two", "un", "under", "until", "up", "upon", "us",
                "very", "via", "was", "we", "well", "were", "what", "whatever", "when", "whence", "whenever", "where", "whereafter", "whereas", "whereby",
                "wherein", "whereupon", "wherever", "whether", "which", "while", "whither", "who", "whoever", "whole", "whom", "whose", "why", "will",
                "with", "within", "without", "would", "yet", "you", "your", "yours", "yourself", "yourselves", "the"}));

        try {
            status = TwitterObjectFactory.createStatus(tweet);                                                              //objeto de twitter
            text = status.getText();                                                                                        //text en el objeto de twitter
            String[] words = text.split("\\s");                                                                          //particion del texto del objeto de twitter
            id = String.valueOf(status.getId());                                                                            //id de ese tweet
            id2 = new Text(id);

            for (String word: words ){                                                                                    //iterar por ese array de palabras
                word = word.replaceAll("[^\\w]","");//elimina caracteres raros

                if (!isStopWord(word,stopWOrds)&& !word.contains("\n") && !word.contains("\t") ) {//si NO es un stopword

                 context.write(new Text(word),new IntWritable(1));                                                     //escribe el 1
             }
            }
        } catch (TwitterException e) {
            e.printStackTrace();
        }
    }

}

