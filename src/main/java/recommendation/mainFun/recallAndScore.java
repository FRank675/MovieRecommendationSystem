package recommendation.mainFun;
import recommendation.dataFactory.Factory;
import recommendation.dataFactory.Movie;
import java.util.*;


public class recallAndScore {

    //多路召回
    public static List<Movie> multipleRetrievalCandidates(int useId){
        List<Movie> historyMovie = Factory.getInstance().getHistoryMovie(useId);
        HashSet<String> genres = new HashSet<>();

        //获取电影风格
        for(Movie movie : historyMovie){
            if(movie != null){
                for (String genre : movie.getGenres()) {
                    genres.add(genre);
                }
            }

        }
        //根据用户看过的电影，统计用户喜欢的电影风格
        HashMap<Integer, Movie> candidateMap = new HashMap<>();
        //根据用户喜欢的风格召回电影候选集
        for (String genre : genres){
            List<Movie> oneCandidates = Factory.getInstance().getMoviesByGenre(genre, 20, "rating");
            for (Movie candidate : oneCandidates){
                candidateMap.put(candidate.getMovieId(), candidate);
            }
        }

        //召回所有电影中排名最高的100部电影
        List<Movie> highRatingCandidates = Factory.getInstance().getMovies(100, "rating");
        for (Movie candidate : highRatingCandidates){
            candidateMap.put(candidate.getMovieId(), candidate);
        }

        //召回最新上映的100部电影
        List<Movie> latestCandidates = Factory.getInstance().getMovies(100, "releaseYear");
        for (Movie candidate : latestCandidates){
            candidateMap.put(candidate.getMovieId(), candidate);
        }

        //去除用户已经观看过的电影
        for (Movie a : historyMovie){
            if(a != null)
                candidateMap.remove(a.getMovieId());
        }
        return new ArrayList<>(candidateMap.values());
    }

    //基于Embedding的召回方法
    public static List<Movie> retrievalCandidatesByEmbedding(Movie movie, int size){
        if (null == movie || null == movie.getEmb()){
            return null;
        }

        //获取评分前10000的电影作为全部候选集
        List<Movie> allCandidates = Factory.getInstance().getMovies(10000, "rating");
        HashMap<Movie,Double> movieScoreMap = new HashMap<>();
        for (Movie candidate : allCandidates){
            //计算与用户观看电影的embedding相似度
            double similarity = calculateEmbSimilarScore(movie, candidate);
            movieScoreMap.put(candidate, similarity);
        }

        List<Map.Entry<Movie,Double>> movieScoreList = new ArrayList<>(movieScoreMap.entrySet());
        //按照用户-电影embedding相似度进行侯选电影集排序
        movieScoreList.sort(Map.Entry.comparingByValue());

        //生成并返回最终的候选集
        List<Movie> candidates = new ArrayList<>();
        for (Map.Entry<Movie,Double> movieScoreEntry : movieScoreList){
            candidates.add(movieScoreEntry.getKey());
        }

        return candidates.subList(0, Math.min(candidates.size(), size));
    }

    //排序
    public static List<Movie> ranker(Movie movie, List<Movie> candidates, String model){
        HashMap<Movie, Double> candidateScoreMap = new HashMap<>();
        for (Movie candidate : candidates){
            double similarity;
            switch (model){
                case "embedding":
                    similarity = calculateEmbSimilarScore(movie, candidate);
                    break;
                default:
                    similarity = calculateSimilarScore(movie, candidate);
            }
            candidateScoreMap.put(candidate, similarity);

        }
        List<Movie> rankedList = new ArrayList<>();
        //对map根据value值倒序排序
        candidateScoreMap.entrySet().stream().sorted(Map.Entry.comparingByValue(Comparator.reverseOrder())).forEach(m -> rankedList.add(m.getKey()));
        return rankedList;
    }

    //按权重计算相似度评分
    public static double calculateSimilarScore(Movie movie, Movie candidate){
        int sameGenreCount = 0;
        for (String genre : movie.getGenres()){
            if (candidate.getGenres().contains(genre)){
                sameGenreCount++;
            }
        }
        //计算类型相似度
        double genreSimilarity = (double)sameGenreCount / (movie.getGenres().size() + candidate.getGenres().size()) / 2;
        //评分总分5分，转换为1分制
        double ratingScore = candidate.getAverageRating() / 5;

        //权重占比
        double similarityWeight = 0.7;
        double ratingScoreWeight = 0.3;

        return genreSimilarity * similarityWeight + ratingScore * ratingScoreWeight;
    }

    //基于Embedding相似度计算
    public static double calculateEmbSimilarScore(Movie movie, Movie candidate){
        if (null == movie || null == candidate){
            return -1;
        }
        //返回余弦相似度
        return movie.getEmb().calculateSimilarity(candidate.getEmb());
    }
}
