package recommendation.dataFactory;
import recommendation.deal.Config;
import recommendation.deal.Utility;
import java.io.File;
import java.util.*;

/**
 * 负责所有的数据加载逻辑。
 */

public class Factory {
    //单例实例
    private static volatile Factory instance;
    //用hashmap存储movie和user的特征
    HashMap<Integer, Movie> movieMap;
    HashMap<Integer, User> userMap;
    HashMap<Integer,Integer> userMovie;
    //用于快速查询一个类型的所有电影
    HashMap<String, List<Movie>> genreReverseIndexMap;

    private Factory(){
        this.movieMap = new HashMap<>();
        this.userMap = new HashMap<>();
        this.genreReverseIndexMap = new HashMap<>();
        this.userMovie = new HashMap<>();
        instance = this;
    }

    //实现单例
    public static Factory getInstance(){
        if (null == instance){
            synchronized (Factory.class){
                if (null == instance){
                    instance = new Factory();
                }
            }
        }
        return instance;
    }

    //从文件系统加载数据，包括电影、评级、链接数据和嵌入矢量等模型数据
    public void loadData(String movieDataPath, String linkDataPath, String ratingDataPath, String movieEmbPath, String userEmbPath, String movieRedisKey, String userRedisKey) throws Exception{
        loadMovieData(movieDataPath);
        loadLinkData(linkDataPath);
        loadRatingData(ratingDataPath);
        loadMovieEmb("G:\\Download\\grpc-java-1.41.0\\grpc-java-1.41.0\\lizaizai\\src\\main\\resources\\modeldata\\item2vecEmb4.csv", movieRedisKey);loadMovieEmb("G:\\Download\\grpc-java-1.41.0\\grpc-java-1.41.0\\lizaizai\\src\\main\\resources\\modeldata\\item2vecEmb2.csv", movieRedisKey);loadMovieEmb("G:\\Download\\grpc-java-1.41.0\\grpc-java-1.41.0\\lizaizai\\src\\main\\resources\\modeldata\\item2vecEmb6.csv", movieRedisKey);loadMovieEmb("G:\\Download\\grpc-java-1.41.0\\grpc-java-1.41.0\\lizaizai\\src\\main\\resources\\modeldata\\item2vecEmb7.csv", movieRedisKey);loadMovieEmb("G:\\Download\\grpc-java-1.41.0\\grpc-java-1.41.0\\lizaizai\\src\\main\\resources\\modeldata\\item2vecEmb5.csv", movieRedisKey);loadMovieEmb("G:\\Download\\grpc-java-1.41.0\\grpc-java-1.41.0\\lizaizai\\src\\main\\resources\\modeldata\\item2vecEmb3.csv", movieRedisKey);loadMovieEmb("G:\\Download\\grpc-java-1.41.0\\grpc-java-1.41.0\\lizaizai\\src\\main\\resources\\modeldata\\item2vecEmb0.csv", movieRedisKey);loadMovieEmb("G:\\Download\\grpc-java-1.41.0\\grpc-java-1.41.0\\lizaizai\\src\\main\\resources\\modeldata\\item2vecEmb8.csv", movieRedisKey);
        if (Config.IS_LOAD_ITEM_FEATURE_FROM_REDIS){
            loadMovieFeatures("mf:");
        }
        loadUserEmb("G:\\Download\\grpc-java-1.41.0\\grpc-java-1.41.0\\lizaizai\\src\\main\\resources\\modeldata\\userEmb5.csv", userRedisKey);loadUserEmb("G:\\Download\\grpc-java-1.41.0\\grpc-java-1.41.0\\lizaizai\\src\\main\\resources\\modeldata\\userEmb4.csv", userRedisKey);loadMovieEmb("G:\\Download\\grpc-java-1.41.0\\grpc-java-1.41.0\\lizaizai\\src\\main\\resources\\modeldata\\item2vecEmb2.csv", movieRedisKey);loadMovieEmb("G:\\Download\\grpc-java-1.41.0\\grpc-java-1.41.0\\lizaizai\\src\\main\\resources\\modeldata\\item2vecEmb7.csv", movieRedisKey);loadMovieEmb("G:\\Download\\grpc-java-1.41.0\\grpc-java-1.41.0\\lizaizai\\src\\main\\resources\\modeldata\\item2vecEmb6.csv", movieRedisKey);loadUserEmb("G:\\Download\\grpc-java-1.41.0\\grpc-java-1.41.0\\lizaizai\\src\\main\\resources\\modeldata\\userEmb3.csv", userRedisKey);loadUserEmb("G:\\Download\\grpc-java-1.41.0\\grpc-java-1.41.0\\lizaizai\\src\\main\\resources\\modeldata\\userEmb0.csv", userRedisKey);loadUserEmb("G:\\Download\\grpc-java-1.41.0\\grpc-java-1.41.0\\lizaizai\\src\\main\\resources\\modeldata\\userEmb8.csv", userRedisKey);

    }

    //从movies.csv中加载电影数据
    private void loadMovieData(String movieDataPath) throws Exception{
        boolean skipFirstLine = true;
        //读取movies.csv文件
        try (Scanner scanner = new Scanner(new File(movieDataPath))) {
            while (scanner.hasNextLine()) {
                String movieRawData = scanner.nextLine();
                //跳过首行头
                if (skipFirstLine){
                    skipFirstLine = false;
                    continue;
                }
                //将读取的数据切割，并赋给电影对象属性
                String[] movieData = movieRawData.split(",");
                if (movieData.length == 3){
                    Movie movie = new Movie();
                    //设置电影id
                    movie.setMovieId(Integer.parseInt(movieData[0]));
                    //设置电影发行时间
                    //trim() 方法用于删除字符串的头尾空白符
                    int releaseYear = parseReleaseYear(movieData[1].trim());
                    if (releaseYear == -1){
                        movie.setTitle(movieData[1].trim());
                    }else{
                        movie.setReleaseYear(releaseYear);
                        movie.setTitle(movieData[1].trim().substring(0, movieData[1].trim().length()-6).trim());
                    }
                    //设置电影类别
                    String genres = movieData[2];
                    //如果电影体裁不为空
                    if (!genres.trim().isEmpty()){
                        String[] genreArray = genres.split("\\|");
                        //添加电影体裁
                        for (String genre : genreArray){
                            movie.addGenre(genre);
                            addMovie2GenreIndex(genre, movie);
                        }
                    }
                    //将得到的电影对象存入电影哈希表中
                    this.movieMap.put(movie.getMovieId(), movie);
                }
            }
        }
    }

    //加载电影embedding
    private void loadMovieEmb(String movieEmbPath, String embKey) throws Exception{
        if (Config.EMB_DATA_SOURCE.equals(Config.DATA_SOURCE_FILE)) {
            try (Scanner scanner = new Scanner(new File(movieEmbPath))) {
                while (scanner.hasNextLine()) {
                    String movieRawEmbData = scanner.nextLine();
                    String[] movieEmbData = movieRawEmbData.split(":");
                    if (movieEmbData.length == 2) {
                        Movie m = getMovieById(Integer.parseInt(movieEmbData[0]));
                        if (null == m) {
                            continue;
                        }
                        m.setEmb(Utility.parseEmbStr(movieEmbData[1]));
                    }
                }
            }
        }else{
            Set<String> movieEmbKeys = Redis.getInstance().keys(embKey + "*");
            for (String movieEmbKey : movieEmbKeys){
                String movieId = movieEmbKey.split(":")[1];
                Movie m = getMovieById(Integer.parseInt(movieId));
                if (null == m) {
                    continue;
                }
                m.setEmb(Utility.parseEmbStr(Redis.getInstance().get(movieEmbKey)));
            }
        }
    }

    //加载电影的特征
    private void loadMovieFeatures(String movieFeaturesPrefix) throws Exception{
        Set<String> movieFeaturesKeys = Redis.getInstance().keys(movieFeaturesPrefix + "*");
        int validFeaturesCount = 0;
        for (String movieFeaturesKey : movieFeaturesKeys){
            String movieId = movieFeaturesKey.split(":")[1];
            Movie m = getMovieById(Integer.parseInt(movieId));
            if (null == m) {
                continue;
            }
            m.setMovieFeatures(Redis.getInstance().hgetAll(movieFeaturesKey));
            validFeaturesCount++;
        }
    }

    //加载用户embedding
    private void loadUserEmb(String userEmbPath, String embKey) throws Exception{
        if (Config.EMB_DATA_SOURCE.equals(Config.DATA_SOURCE_FILE)) {
            try (Scanner scanner = new Scanner(new File(userEmbPath))) {
                while (scanner.hasNextLine()) {
                    String userRawEmbData = scanner.nextLine();
                    String[] userEmbData = userRawEmbData.split(":");
                    if (userEmbData.length == 2) {
                        User u = getUserById(Integer.parseInt(userEmbData[0]));
                        if (null == u) {
                            continue;
                        }
                        u.setEmb(Utility.parseEmbStr(userEmbData[1]));
                    }
                }
            }
        }
    }

    //获取电影上映年份
    private int parseReleaseYear(String rawTitle){
        if (null == rawTitle || rawTitle.trim().length() < 6){
            return -1;
        }else{
            String yearString = rawTitle.trim().substring(rawTitle.length()-5, rawTitle.length()-1);
            try{
                return Integer.parseInt(yearString);
            }catch (NumberFormatException exception){
                return -1;
            }
        }
    }

    private void loadLinkData(String linkDataPath) throws Exception{
        boolean skipFirstLine = true;
        try (Scanner scanner = new Scanner(new File(linkDataPath))) {
            while (scanner.hasNextLine()) {
                String linkRawData = scanner.nextLine();
                if (skipFirstLine){
                    skipFirstLine = false;
                    continue;
                }
                String[] linkData = linkRawData.split(",");
                if (linkData.length == 3){
                    int movieId = Integer.parseInt(linkData[0]);
                    Movie movie = this.movieMap.get(movieId);
                    if (null != movie){
                        movie.setImdbId(linkData[1].trim());
                        movie.setTmdbId(linkData[2].trim());
                    }
                }
            }
        }
    }

    private void loadRatingData(String ratingDataPath) throws Exception{
        boolean skipFirstLine = true;
        try (Scanner scanner = new Scanner(new File(ratingDataPath))) {
            while (scanner.hasNextLine()) {
                String ratingRawData = scanner.nextLine();
                if (skipFirstLine){
                    skipFirstLine = false;
                    continue;
                }
                String[] linkData = ratingRawData.split(",");
                if (linkData.length == 4){
                    Rating rating = new Rating();
                    rating.setUserId(Integer.parseInt(linkData[0]));
                    rating.setMovieId(Integer.parseInt(linkData[1]));
                    rating.setScore(Float.parseFloat(linkData[2]));
                    rating.setTimestamp(Long.parseLong(linkData[3]));
                    Movie movie = this.movieMap.get(rating.getMovieId());
                    if (null != movie){
                        movie.addRating(rating);
                    }
                    if (!this.userMap.containsKey(rating.getUserId())){
                        User user = new User();
                        user.setUserId(rating.getUserId());
                        this.userMap.put(user.getUserId(), user);

                    }
                    this.userMovie.put(rating.getUserId(),rating.getMovieId());
                    this.userMap.get(rating.getUserId()).addRating(rating);

                }
            }
        }

    }

    private void addMovie2GenreIndex(String genre, Movie movie){
        if (!this.genreReverseIndexMap.containsKey(genre)){
            this.genreReverseIndexMap.put(genre, new ArrayList<>());
        }
        this.genreReverseIndexMap.get(genre).add(movie);
    }

    //通过指定体裁获取电影，并且将所得电影按照指定的方法进行排序
    public List<Movie> getMoviesByGenre(String genre, int size, String sortBy){
        if (null != genre){
            List<Movie> movies = new ArrayList<>(this.genreReverseIndexMap.get(genre));
            switch (sortBy){
                case "rating":movies.sort((m1, m2) -> Double.compare(m2.getAverageRating(), m1.getAverageRating()));break;
                case "releaseYear": movies.sort((m1, m2) -> Integer.compare(m2.getReleaseYear(), m1.getReleaseYear()));break;
                default:
            }

            if (movies.size() > size){
                return movies.subList(0, size);
            }
            return movies;
        }
        return null;
    }

    //获取指定方法排序得到的前size个电影
    public List<Movie> getMovies(int size, String sortBy){
            List<Movie> movies = new ArrayList<>(movieMap.values());
            switch (sortBy){
                case "rating":movies.sort((m1, m2) -> Double.compare(m2.getAverageRating(), m1.getAverageRating()));break;
                case "releaseYear": movies.sort((m1, m2) -> Integer.compare(m2.getReleaseYear(), m1.getReleaseYear()));break;
                default:
            }

            if (movies.size() > size){
                return movies.subList(0, size);
            }
            return movies;
    }

    //通过电影id获取电影
    public Movie getMovieById(int movieId){
        return this.movieMap.get(movieId);
    }

    //通过用户id获取用户
    public User getUserById(int userId){
        return this.userMap.get(userId);
    }

    public List<Movie> getHistoryMovie(int userId){
        List<Movie> movies = new LinkedList<Movie>();
        List intSet = Collections.singletonList(userMovie.get(userId));
        for(Object a : intSet){
            movies.add(this.movieMap.get(a));
        }
        return movies;
    }
}
