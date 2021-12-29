package recommendation;
import recommendation.dataFactory.Factory;
import recommendation.dataFactory.Movie;
import recommendation.mainFun.UserRecommend;
import java.io.*;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Scanner;
import java.util.Set;


public class RecommendationTest {

    public static void main(String[] args) throws Exception {
        new RecommendationTest().run();
    }

    public void run() throws Exception{

        Factory.getInstance().loadData("G:\\Download\\grpc-java-1.41.0\\grpc-java-1.41.0\\lizaizai\\src\\main\\resources\\data\\movies.csv", "G:\\Download\\grpc-java-1.41.0\\grpc-java-1.41.0\\lizaizai\\src\\main\\resources\\data\\links.csv", "C:\\Users\\Lenovo\\Desktop\\ratings_20%.csv", "G:\\Download\\grpc-java-1.41.0\\grpc-java-1.41.0\\lizaizai\\src\\main\\resources\\modeldata\\item2vecEmb.csv", "G:\\Download\\grpc-java-1.41.0\\grpc-java-1.41.0\\lizaizai\\src\\main\\resources\\modeldata\\userEmb.csv", "i2vEmb", "uEmb");

        UserRecommend rec = new UserRecommend();
        Set linkedHashSet = new LinkedHashSet();

        try (FileInputStream inputStream = new FileInputStream("G:\\Download\\grpc-java-1.41.0\\grpc-java-1.41.0\\lizaizai\\src\\main\\resources\\data\\ratings.csv")){
            Scanner sc = new Scanner(inputStream);
            String line = sc.nextLine();
            while (sc.hasNextLine()){
                line = sc.nextLine();
                String[] strArray = line.split(",");
                String key = strArray[0];
                linkedHashSet.add(key);
            }

        }catch (Exception e){
            e.printStackTrace();
        }

        File writeFile = new File("G:\\aaaaWrite.csv");
        BufferedWriter writeText = new BufferedWriter(new FileWriter(writeFile));

        for(Object id:linkedHashSet) {//这里再使用增强型for循环
            List<Movie> can =  rec.getRecList( Integer.parseInt(id.toString()),5,"embedding");
            //第一步：设置输出的文件路径
            //如果该目录下不存在该文件，则文件会被创建到指定目录下。如果该目录有同名文件，那么该文件将被覆盖。
            try{
                //第二步：通过BufferedReader类创建一个使用默认大小输出缓冲区的缓冲字符输出流
                for(int i =0;i<5;i++) {
                    //第三步：将文档的下一行数据赋值给lineData，并判断是否为空，若不为空则输出
                    String[] b  = can.get(i).toString().split(" ");
                    writeText.newLine();    //换行
                    writeText.write(id + ","+b[0]+","+b[1]);
                }
                //使用缓冲区的刷新方法将数据刷到目的地中
                writeText.flush();
                //关闭缓冲区，缓冲区没有调用系统底层资源，真正调用底层资源的是FileWriter对象，缓冲区仅仅是一个提高效率的作用
                //因此，此处的close()方法关闭的是被缓存的流对象
            }catch (FileNotFoundException e){
                System.out.println("没有找到指定文件");
            }catch (IOException e){
                System.out.println("文件读写出错");
            }
        }
        writeText.close();
        }
}
