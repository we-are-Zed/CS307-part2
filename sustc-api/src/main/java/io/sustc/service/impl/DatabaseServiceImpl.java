package io.sustc.service.impl;

import io.sustc.dto.DanmuRecord;
import io.sustc.dto.UserRecord;
import io.sustc.dto.VideoRecord;
import io.sustc.service.DatabaseService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * It's important to mark your implementation class with {@link Service} annotation.
 * As long as the class is annotated and implements the corresponding interface, you can place it under any package.
 */
@Service
@Slf4j
@EnableAsync
public class DatabaseServiceImpl implements DatabaseService {

    /**
     * Getting a {@link DataSource} instance from the framework, whose connections are managed by HikariCP.
     * <p>
     * Marking a field with {@link Autowired} annotation enables our framework to automatically
     * provide you a well-configured instance of {@link DataSource}.
     * Learn more: <a href="https://www.baeldung.com/spring-dependency-injection">Dependency Injection</a>
     */
    @Autowired
    private DataSource dataSource;

    @Autowired
    private AsyncTaskExecutor asyncTaskExecutor;

    @Override
    public List<Integer> getGroupMembers() {
        return Arrays.asList(12210161, 12210532, 12212231);
    }

    @Override
    public void importData(
            List<DanmuRecord> danmuRecords,
            List<UserRecord> userRecords,
            List<VideoRecord> videoRecords
    ) {
        CountDownLatch counter = new CountDownLatch(3);
        asyncTaskExecutor.submit(() -> {
            importUserRecords(userRecords);
            counter.countDown();
        });
        asyncTaskExecutor.submit(() -> {
            importVideoRecords(videoRecords);
            counter.countDown();
        });
        asyncTaskExecutor.submit(() -> {
            importDanmuRecords(danmuRecords);
            counter.countDown();
        });
        try {
            counter.await();
            Connection conn = dataSource.getConnection();
            PreparedStatement foreign_key = conn.prepareStatement("ALTER TABLE followings ADD CONSTRAINT foledmid_fk FOREIGN KEY (followedMid) REFERENCES users(mid) ON DELETE CASCADE;\n" +
                    "ALTER TABLE followings ADD CONSTRAINT folermid_fk FOREIGN KEY (followerMid) REFERENCES users(mid) ON DELETE CASCADE;" +
                    "ALTER TABLE videolike ADD CONSTRAINT likebv_fk FOREIGN KEY (bv) REFERENCES videos(bv) ON DELETE CASCADE;\n" +
                    "ALTER TABLE videolike ADD CONSTRAINT likemid_fk FOREIGN KEY (mid) REFERENCES users(mid) ON DELETE CASCADE;" +
                    "ALTER TABLE coin ADD CONSTRAINT bv_fk FOREIGN KEY (bv) REFERENCES videos(bv) ON DELETE CASCADE;\n" +
                    "ALTER TABLE coin ADD CONSTRAINT mid_fk FOREIGN KEY (mid) REFERENCES users(mid) ON DELETE CASCADE;" +
                    "ALTER TABLE favorite ADD CONSTRAINT favbv_fk FOREIGN KEY (bv) REFERENCES videos(bv) ON DELETE CASCADE;\n" +
                    "ALTER TABLE favorite ADD CONSTRAINT favmid_fk FOREIGN KEY (mid) REFERENCES users(mid) ON DELETE CASCADE;" +
                    "ALTER TABLE danmulike ADD CONSTRAINT danmuid_fk FOREIGN KEY (danmuId) REFERENCES danmus(id) ON DELETE CASCADE;\n" +
                    "ALTER TABLE danmulike ADD CONSTRAINT danmid_fk FOREIGN KEY (mid) REFERENCES users(mid) ON DELETE CASCADE;" +
                    "ALTER TABLE view ADD CONSTRAINT viewbv_fk FOREIGN KEY (bv) REFERENCES videos(bv) ON DELETE CASCADE;\n" +
                    "ALTER TABLE view ADD CONSTRAINT viewmid_fk FOREIGN KEY (mid) REFERENCES users(mid) ON DELETE CASCADE;");
            foreign_key.execute();
        } catch (InterruptedException | SQLException e) {
            throw new RuntimeException(e);
        }
        System.out.println(danmuRecords.size());
        System.out.println(userRecords.size());
        System.out.println(videoRecords.size());
    }

    @Async
    public void importUserRecords(List<UserRecord> userRecords){
        String sql="INSERT INTO users (mid, name, sex, birthday, level, coin, sign, identity, password, qq, wechat) VALUES (?,?,?,?,?,?,?,?,?,?,?)";
        String sqlFollowing = "INSERT INTO followings (followedmid, followermid) VALUES (?, ?)";
        try(Connection conn=dataSource.getConnection();
            PreparedStatement stmt=conn.prepareStatement(sql);
            PreparedStatement stmtFollowing = conn.prepareStatement(sqlFollowing)) {

            conn.setAutoCommit(false);
            int count1 = 0, count2 = 0;
            final int BATCH_SIZE = 1000;

         for(UserRecord record : userRecords){
             stmt.setLong(1,record.getMid());
             stmt.setString(2,record.getName());
             stmt.setString(3,record.getSex());
             stmt.setString(4,record.getBirthday());
             stmt.setShort(5,record.getLevel());
             stmt.setInt(6,record.getCoin());
             stmt.setString(7,record.getSign());
             stmt.setString(8,record.getIdentity().toString());
             stmt.setString(9,record.getPassword());
             stmt.setString(10,record.getQq());
             stmt.setString(11,record.getWechat());
             stmt.addBatch();
                count1++;
                if(count1 % BATCH_SIZE==0) {
                    stmt.executeBatch();
                    stmt.clearBatch();
                    conn.commit();
                }

         }
            if(count1 > 0) {
                stmt.executeBatch();
                stmt.clearBatch();
            }
            conn.commit();

            for (UserRecord record : userRecords){
                if(record.getFollowing()!=null){
                    for(long followedMid : record.getFollowing()){
                        stmtFollowing.setLong(2,followedMid);
                        stmtFollowing.setLong(1,record.getMid());
                        stmtFollowing.addBatch();
                        count2++;

                        if(count2 % BATCH_SIZE==0) {
                            stmtFollowing.executeBatch();
                            stmtFollowing.clearBatch();
                            conn.commit();
                        }
                    }
                }
                if(count2 % BATCH_SIZE==0) {
                    stmtFollowing.executeBatch();
                    stmtFollowing.clearBatch();
                    conn.commit();
                }
            }
            if(count2 > 0) {
                stmtFollowing.executeBatch();
                stmtFollowing.clearBatch();
            }
            conn.commit();
        } catch (SQLException e) {
         throw new RuntimeException(e);
        }
    }

    @Async
    public void importVideoRecords(List<VideoRecord> videoRecords){
     String sql="INSERT INTO videos (bv, title, ownermid, ownername, committime, reviewtime, publictime, duration, description, reviewer) VALUES (?,?,?,?,?,?,?,?,?,?)";
     String sqlVideoLike = "INSERT INTO videolike (bv, mid) VALUES (?, ?)";
     String sqlCoin = "INSERT INTO coin (bv, mid) VALUES (?, ?)";
     String sqlFavorite = "INSERT INTO favorite (bv, mid) VALUES (?, ?)";
     String sqlView = "INSERT INTO view (bv, mid, viewTime) VALUES (?, ?, ?)";

     try(Connection conn=dataSource.getConnection();
     PreparedStatement stmt=conn.prepareStatement(sql);
     PreparedStatement stmtVideoLike = conn.prepareStatement(sqlVideoLike);
     PreparedStatement stmtCoin = conn.prepareStatement(sqlCoin);
     PreparedStatement stmtFavorite = conn.prepareStatement(sqlFavorite);
     PreparedStatement stmtView = conn.prepareStatement(sqlView)) {
         conn.setAutoCommit(false);
         int count1 = 0, count2 = 0,count3 = 0, count4 = 0, count5 = 0;
         final int BATCH_SIZE = 1000;
         for(VideoRecord record : videoRecords){
             stmt.setString(1,record.getBv());
             stmt.setString(2,record.getTitle());
             stmt.setLong(3,record.getOwnerMid());
             stmt.setString(4,record.getOwnerName());
             stmt.setTimestamp(5,record.getCommitTime());
             stmt.setTimestamp(6,record.getReviewTime());
             stmt.setTimestamp(7,record.getPublicTime());
             stmt.setFloat(8,record.getDuration());
             stmt.setString(9,record.getDescription());
             stmt.setLong(10,record.getReviewer());
             stmt.addBatch();
             count1++;
             if(count1 % BATCH_SIZE==0) {
                 stmt.executeBatch();
                 stmt.clearBatch();
                 conn.commit();
             }

         }

         if(count1 % BATCH_SIZE!=0) {
             stmt.executeBatch();
             stmt.clearBatch();
         }
         conn.commit();


         for(VideoRecord record:videoRecords){
             if(record.getLike()!=null){
                 for(long mid : record.getLike()){
                     stmtVideoLike.setString(1,record.getBv());
                     stmtVideoLike.setLong(2,mid);
                     stmtVideoLike.addBatch();
                     count2++;

                     if(count2 % BATCH_SIZE==0) {
                         stmtVideoLike.executeBatch();
                         stmtVideoLike.clearBatch();
                         conn.commit();
                     }
                 }
             }
             if(count2 % BATCH_SIZE == 0) {
                 stmtVideoLike.executeBatch();
                 stmtVideoLike.clearBatch();
                 conn.commit();
             }

             if(record.getCoin() != null){
                 for(long mid : record.getCoin()){
                     stmtCoin.setString(1,record.getBv());
                     stmtCoin.setLong(2,mid);
                     stmtCoin.addBatch();
                     count3++;

                     if(count3 % BATCH_SIZE == 0) {
                         stmtCoin.executeBatch();
                         stmtCoin.clearBatch();
                         conn.commit();
                     }
                 }
             }
             if(count3 % BATCH_SIZE == 0) {
                 stmtCoin.executeBatch();
                 stmtCoin.clearBatch();
                 conn.commit();
             }

             if(record.getFavorite()!=null){
                 for(long mid : record.getFavorite()){
                     stmtFavorite.setString(1,record.getBv());
                     stmtFavorite.setLong(2,mid);
                     stmtFavorite.addBatch();
                     count4++;

                     if(count4 % BATCH_SIZE == 0) {
                         stmtFavorite.executeBatch();
                         stmtFavorite.clearBatch();
                         conn.commit();
                     }

                 }
             }
             if(count4 % BATCH_SIZE == 0) {
                 stmtFavorite.executeBatch();
                 stmtFavorite.clearBatch();
                 conn.commit();
             }

             if(record.getViewerMids()!=null&&record.getViewTime()!=null){
                 for(int i=0;i<record.getViewerMids().length;i++){
                     stmtView.setString(1,record.getBv());
                     stmtView.setLong(2,record.getViewerMids()[i]);
                     stmtView.setFloat(3,record.getViewTime()[i]);
                     stmtView.addBatch();
                     count5++;

                     if(count5 % BATCH_SIZE == 0) {
                         stmtView.executeBatch();
                         stmtView.clearBatch();
                         conn.commit();
                     }
                 }
             }
             if(count5 % BATCH_SIZE == 0) {
                 stmtView.executeBatch();
                 stmtView.clearBatch();
                 conn.commit();
             }

         }

            if(count2 % BATCH_SIZE > 0) {
                stmtVideoLike.executeBatch();
                stmtVideoLike.clearBatch();
            }
            if(count3 % BATCH_SIZE > 0) {
                stmtCoin.executeBatch();
                stmtCoin.clearBatch();
            }
            if(count4 % BATCH_SIZE > 0) {
                stmtFavorite.executeBatch();
                stmtFavorite.clearBatch();
            }
            if(count5 % BATCH_SIZE > 0) {
                stmtView.executeBatch();
                stmtView.clearBatch();
            }
            conn.commit();

     }catch (SQLException e){
         throw new RuntimeException(e);
     }
    }

    @Async
    public void importDanmuRecords(List<DanmuRecord> danmuRecords){
        String sqlDanmu = "INSERT INTO danmus (bv, mid, time, content, posttime) VALUES (?, ?, ?, ?, ?) RETURNING id";
        String sqlDanmuLike = "INSERT INTO danmulike (danmuid, mid) VALUES (?, ?)";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmtDanmu = conn.prepareStatement(sqlDanmu, Statement.RETURN_GENERATED_KEYS);
             PreparedStatement stmtDanmuLike = conn.prepareStatement(sqlDanmuLike)) {
            conn.setAutoCommit(false);

            int count1 = 0, count2 = 0;
            final int BATCH_SIZE = 1000;
            for (DanmuRecord record : danmuRecords) {
                stmtDanmu.setString(1, record.getBv());
                stmtDanmu.setLong(2, record.getMid());
                stmtDanmu.setFloat(3, record.getTime());
                stmtDanmu.setString(4, record.getContent());
                stmtDanmu.setTimestamp(5, record.getPostTime());
                stmtDanmu.executeUpdate();

                try (ResultSet generatedKeys = stmtDanmu.getGeneratedKeys()) {
                    if (generatedKeys.next()) {
                        long danmuId = generatedKeys.getLong(1);

                        count2=processDanmuLikes(record.getLikedBy(), danmuId, stmtDanmuLike,count2,conn);
                    }
                }
                if(count2 % BATCH_SIZE==0){
                    stmtDanmuLike.executeBatch();
                    stmtDanmuLike.clearBatch();
                    conn.commit();
                }

            }
            if(count2 % BATCH_SIZE!=0){
                stmtDanmuLike.executeBatch();
                stmtDanmuLike.clearBatch();
            }
            conn.commit();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private int processDanmuLikes(long[] likedBy, long danmuId, PreparedStatement stmt,int count2,Connection conn) throws SQLException {
        if (likedBy != null) {
            for (long mid : likedBy) {
                stmt.setLong(1, danmuId);
                stmt.setLong(2, mid);
                stmt.addBatch();
                count2++;
                if (count2 % 1000 == 0) {
                    stmt.executeBatch();
                    stmt.clearBatch();
                    conn.commit();
                }
            }
        }
        return count2;
    }

    /*
     * The following code is just a quick example of using jdbc datasource.
     * Practically, the code interacts with database is usually written in a DAO layer.
     *
     * Reference: [Data Access Object pattern](https://www.baeldung.com/java-dao-pattern)
     */

    @Override
    public void truncate() {
        // You can use the default truncate script provided by us in most cases,
        // but if it doesn't work properly, you may need to modify it.

        String sql = "DO $$\n" +
                "DECLARE\n" +
                "    tables CURSOR FOR\n" +
                "        SELECT tablename\n" +
                "        FROM pg_tables\n" +
                "        WHERE schemaname = 'public';\n" +
                "BEGIN\n" +
                "    FOR t IN tables\n" +
                "    LOOP\n" +
                "        EXECUTE 'TRUNCATE TABLE ' || QUOTE_IDENT(t.tablename) || ' CASCADE;';\n" +
                "    END LOOP;\n" +
                "END $$;\n";

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Integer sum(int a, int b) {
        String sql = "SELECT ?+?";

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setInt(1, a);
            stmt.setInt(2, b);
            log.info("SQL: {}", stmt);

            ResultSet rs = stmt.executeQuery();
            rs.next();
            return rs.getInt(1);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
