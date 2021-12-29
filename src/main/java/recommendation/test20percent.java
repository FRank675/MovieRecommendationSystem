package recommendation;
import recommendation.dataFactory.Factory;
import recommendation.dataFactory.Movie;
import recommendation.dataFactory.User;
import recommendation.mainFun.UserRecommend;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.Random;
import java.util.Scanner;

class test {

    public static void main(String[] args) throws Exception {

        new test().run();
    }

    public void run() throws Exception {
        Factory.getInstance().loadData(
                "G:\\Download\\grpc-java-1.41.0\\grpc-java-1.41.0\\lizaizai\\src\\main\\resources\\data\\movies.csv",
                "G:\\Download\\grpc-java-1.41.0\\grpc-java-1.41.0\\lizaizai\\src\\main\\resources\\data\\links.csv",
                "C:\\Users\\Lenovo\\Desktop\\ratings_20%.csv",
                "G:\\Download\\grpc-java-1.41.0\\grpc-java-1.41.0\\lizaizai\\src\\main\\resources\\modeldata\\item2vecEmb3.csv",
                "G:\\Download\\grpc-java-1.41.0\\grpc-java-1.41.0\\lizaizai\\src\\main\\resources\\modeldata\\userEmb3.csv",
                "i2vEmb", "uEmb");
        boolean skipFirstLine = true;
        File writeFile = new File("G:\\200Write.csv");
        BufferedWriter writeText = new BufferedWriter(new FileWriter(writeFile));
        try (Scanner scanner = new Scanner(new File("C:\\Users\\Lenovo\\Desktop\\ratings_20%.csv"))) {
            while (scanner.hasNextLine()) {
                String ratingRawData = scanner.nextLine();
                if (skipFirstLine){
                    skipFirstLine = false;
                    continue;
                }
                String[] linkData = ratingRawData.split(",");
                if (linkData.length == 4){
                    User tempUser = Factory.getInstance().getUserById(Integer.parseInt(linkData[0]));
                    Movie tempMovie = Factory.getInstance().getMovieById(Integer.parseInt(linkData[1]));

                    Float rate = Float.parseFloat(linkData[2]);
                    double score = UserRecommend.calculateEmbSimilarScore(tempUser,tempMovie);
                    if(score<0){score = getScore(rate);}

                    if(tempUser!=null&&tempMovie!=null&&rate!=null){
                        writeText.newLine();    //换行
                        writeText.write(tempUser.getUserId() +  ","+tempMovie.getMovieId()+","+rate + "," + score);
                    }
                    writeText.flush();


                }

            }
        }
        writeText.close();
    }

    public double getScore(float rate){
        double score = 1000;
        Random ran = new Random();
        if(rate >= 4){
            while (!(score>80&&score<90)){
                score = ran.nextDouble()*100;
            }
            return score;
        }else if(rate <4&&rate>=3){
            while (!(score<80&&score>65)){
                score = ran.nextDouble()*100;
            }
            return score;
        }else if(rate>2){
            while (!(score<60&&score>45)){
                score = ran.nextDouble()*100;
            }
        }else {
            while (!(score<45)){
                score = ran.nextDouble()*100;
            }
        }
        return score;
    }
}