package io.sustc.service.impl;

import io.sustc.dto.*;
import io.sustc.service.DatabaseService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import io.sustc.service.VideoService;
import org.springframework.stereotype.Service;
import javax.sql.DataSource;
import java.sql.*;

import java.util.*;
import java.util.random.*;
import io.sustc.service.impl.*;

@Service
@Slf4j
public class VideoServiceImpI implements VideoService{
    @Autowired
    private DataSource dataSource;

    private boolean isValidAuth(AuthInfo auth) {
        try (Connection conn = dataSource.getConnection()) {
            // 如果提供了 mid 和 password，先验证它们
            if (auth.getMid() > 0 && auth.getPassword() != null) {
                if (!validateMidAndPassword(conn, auth.getMid(), auth.getPassword())) {
                    return false;
                }
            }

            // 如果同时提供了 qq 和 wechat
            if (auth.getQq() != null && !auth.getQq().isEmpty() && auth.getWechat() != null && !auth.getWechat().isEmpty()) {
                if( !validateQqAndWechat(conn, auth.getQq(), auth.getWechat())){
                    return false;
                }
            }
                // 单独验证 qq 或 wechat
                if (auth.getQq() != null && !auth.getQq().isEmpty()&&auth.getWechat()==null) {
                    if(!validateQqOrWechat(conn, "qq", auth.getQq())){
                        return false;
                    }
                }
                if (auth.getWechat() != null && !auth.getWechat().isEmpty()&&auth.getQq()==null) {
                    if(!validateQqOrWechat(conn, "wechat", auth.getWechat())){
                        return false;
                    }
                }

            conn.close();
            return true;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private boolean validateMidAndPassword(Connection conn, long mid, String password) throws SQLException {
        String sql = "SELECT password FROM users WHERE mid = ?";
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setLong(1, mid);
            ResultSet rs = stmt.executeQuery();
            if (rs.next()) {
                return rs.getString("password").equals(password);
            }else {
                return false;
            }
        }
    }

    private boolean validateQqAndWechat(Connection conn, String qq, String wechat) throws SQLException {
        String sql = "SELECT COUNT(*) FROM users WHERE qq = ? AND wechat = ?";
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, qq);
            stmt.setString(2, wechat);
            ResultSet rs = stmt.executeQuery();
            if (rs.next()) {
                return rs.getInt(1) == 1; // 确保 qq 和 wechat 属于同一个用户
            }
            return false;
        }
    }
    private boolean validateQqOrWechat(Connection conn, String field, String value) throws SQLException {
        String sql = "SELECT COUNT(*) FROM users WHERE " + field + " = ?";
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, value);
            ResultSet rs = stmt.executeQuery();
            if (rs.next()) {
                return rs.getInt(1) > 0;
            }
            return false;
        }
    }


    @Override
    public String postVideo(AuthInfo auth, PostVideoReq req) {
        try(Connection conn = dataSource.getConnection()){

            if(!isValidAuth(auth)){
                return null;
            }
            if(!isValidVideoReq(conn,req,auth.getMid())){
                return null;
            }
            String bv=generateNewBv(conn);
            String ownerName = getUserName(conn, auth.getMid());
            Timestamp now = new Timestamp(System.currentTimeMillis());
            String sql = "INSERT INTO videos (bv, title, description, duration, publictime, ownermid, ownername, committime) VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
            try(PreparedStatement stmt = conn.prepareStatement(sql)){
                stmt.setString(1,bv);
                stmt.setString(2,req.getTitle());
                stmt.setString(3,req.getDescription());
                stmt.setFloat(4,req.getDuration());
                stmt.setTimestamp(5,req.getPublicTime());
                stmt.setLong(6,auth.getMid());
                stmt.setString(7,ownerName);
                stmt.setTimestamp(8,now);
                stmt.executeUpdate();

                return bv;
            }
        }catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
    private String getUserName(Connection conn, long mid) throws SQLException {
        String sql = "SELECT name FROM users WHERE mid = ?";
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setLong(1, mid);
            ResultSet rs = stmt.executeQuery();
            if (rs.next()) {
                return rs.getString("name");
            }
        }
        return null;
    }
    private boolean isValidVideoReq(Connection conn,PostVideoReq req,long ownerMid) throws SQLException {
        if (req.getTitle() == null || req.getTitle().isEmpty()) {
            return false;
        }
        if (isTitleDuplicate(conn, req.getTitle(), ownerMid)) {
            return false;
        }
        if(req.getDuration()<10){
            return false;
        }
        if (req.getPublicTime() != null && req.getPublicTime().before(new Timestamp(System.currentTimeMillis()))) {
            return false;
        }
        if(req.getPublicTime()==null){
            return false;
        }
        return true;

    }
    private boolean isTitleDuplicate(Connection conn, String title, long ownerMid) throws SQLException {
        String sql = "SELECT COUNT(*) FROM videos WHERE title = ? AND ownermid = ?";
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, title);
            stmt.setLong(2, ownerMid);
            ResultSet rs = stmt.executeQuery();
            if (rs.next()) {
                return rs.getInt(1) > 0;
            }
        }catch (SQLException e) {
            throw new RuntimeException(e);
        }

        return false;
    }


    private String generateNewBv(Connection conn) throws SQLException {
        Random random = new Random();
        String newBv;
        do {
            newBv = "BV" + generateRandomDigits(5) + generateRandomLetters(5);
        } while (bvExists(conn, newBv));
        return newBv;
    }

    private String generateRandomDigits(int length) {
        Random random = new Random();
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < length; i++) {
            sb.append(random.nextInt(10));
        }
        return sb.toString();
    }

    private String generateRandomLetters(int length) {
        Random random = new Random();
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < length; i++) {
            char letter = (char) (random.nextInt(26) + (random.nextBoolean() ? 'A' : 'a'));
            sb.append(letter);
        }
        return sb.toString();
    }

    private boolean bvExists(Connection conn, String bv) throws SQLException {
        String sql = "SELECT COUNT(*) FROM videos WHERE bv = ?";
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, bv);
            ResultSet rs = stmt.executeQuery();
            if (rs.next()) {
                return rs.getInt(1) > 0;
            }
        }
        return false;
    }


    @Override
    public boolean deleteVideo(AuthInfo auth, String bv) {
        try(Connection conn = dataSource.getConnection()){
            if(checkMid(auth)){
                auth.setMid(getMid(auth));
            }
        if(!isValidAuth(auth)){
            return false;
        }
        if (!doesVideoExist(conn, bv)) {
            return false;
        }

        if (!isAuthorizedToDeleteVideo(conn, auth, bv)) {
            return false;
        }
            String sqlDeleteDanmus = "DELETE FROM danmus WHERE bv = ?";
            try (PreparedStatement stmtDanmus = conn.prepareStatement(sqlDeleteDanmus)) {
                stmtDanmus.setString(1, bv);
                stmtDanmus.executeUpdate();
            }

        String sql= "DELETE FROM videos WHERE bv = ?";
        try(PreparedStatement stmt = conn.prepareStatement(sql)){
            stmt.setString(1,bv);
            stmt.executeUpdate();
            conn.close();
            return true;
        }
    }catch (SQLException e) {
        throw new RuntimeException(e);
    }

    }
    private boolean doesVideoExist(Connection conn, String bv) throws SQLException {
        String sql = "SELECT COUNT(*) FROM videos WHERE bv = ?";
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, bv);
            ResultSet rs = stmt.executeQuery();
            if (rs.next()) {
                if(rs.getInt(1)>0){
                    return true;
                }else {
                    return false;
                }
            }
        }
        return false;
    }

    private boolean isAuthorizedToDeleteVideo(Connection conn, AuthInfo auth, String bv) throws SQLException {
        // 检查用户是否为超级用户
        if (isSuperUser(conn, auth.getMid())) {
            return true;
        }

        // 检查用户是否为视频所有者
        return isOwnerOfVideo(conn, auth.getMid(), bv);
    }

    private boolean isSuperUser(Connection conn, long mid) throws SQLException {
        String sql = "SELECT identity FROM users WHERE mid = ?";
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setLong(1, mid);
            ResultSet rs = stmt.executeQuery();
            if (rs.next()) {
                String identity = rs.getString("identity");
                return "SUPERUSER".equals(identity);
            }
        }
        return false;
    }

    private boolean isOwnerOfVideo(Connection conn, long mid, String bv) throws SQLException {
        String sql = "SELECT ownermid FROM videos WHERE bv = ?";
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, bv);
            ResultSet rs = stmt.executeQuery();
            if (rs.next()) {
                long ownerMid = rs.getLong("ownermid");
                return mid == ownerMid;
            }
        }
        return false;
    }


    @Override
    public boolean updateVideoInfo(AuthInfo auth, String bv, PostVideoReq req) {
        try(Connection conn=dataSource.getConnection()){
            if(checkMid(auth)){
                auth.setMid(getMid(auth));
            }
            if(!isValidAuth(auth)){
                return false;
            }
            if(!doesVideoExist(conn,bv)){
                return false;
            }
            if(!isOwnerOfVideo(conn,auth.getMid(),bv)){
                return false;
            }
            if (!isValidUpdateReq(conn, bv, req,auth)) {
                return false;
            }
            if(req.getPublicTime()==null){
                return false;
            }
            if(req.getPublicTime().before(gerPublicTime(conn,bv))){
                return false;
            }

            Timestamp now = new Timestamp(System.currentTimeMillis());
            Timestamp reviewTime=getReviewTime(conn,bv);
            long lastReviewer=getReviewer(conn,bv);
            if(req.getPublicTime().before(now)){
                return false;
            }

            String sql = "UPDATE videos SET title = ?, description = ?, duration = ?, publictime = ?, committime = ?, reviewtime = ?, reviewer=? WHERE bv = ?";
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
                stmt.setString(1, req.getTitle());
                stmt.setString(2, req.getDescription());
                stmt.setFloat(3, req.getDuration());
                stmt.setTimestamp(4, req.getPublicTime());
                stmt.setTimestamp(5, now);
            stmt.setTimestamp(6,null);
            stmt.setLong(7,0);
                stmt.setString(8, bv);
                stmt.executeUpdate();
            }
           if(reviewTime==null&&lastReviewer==0){
               return false;
           }
           return true;

        }catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
    private Timestamp getReviewTime(Connection conn, String bv) throws SQLException {
        String sql = "SELECT reviewtime FROM videos WHERE bv = ?";
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, bv);
            ResultSet rs = stmt.executeQuery();
            if (rs.next()) {
                return rs.getTimestamp("reviewtime");
            }
        }
        return null;
    }
    private long getReviewer(Connection conn, String bv) throws SQLException {
        String sql = "SELECT reviewer FROM videos WHERE bv = ?";
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, bv);
            ResultSet rs = stmt.executeQuery();
            if (rs.next()) {
                return rs.getLong("reviewer");
            }
        }
        return -1;
    }

    private Timestamp gerPublicTime(Connection conn, String bv) throws SQLException {
        String sql = "SELECT publictime FROM videos WHERE bv = ?";
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, bv);
            ResultSet rs = stmt.executeQuery();
            if (rs.next()) {
                return rs.getTimestamp("publictime");
            }
        }
        return null;
    }

    private boolean isValidUpdateReq(Connection conn, String bv, PostVideoReq req, AuthInfo auth) throws SQLException {
        // 检查标题是否重复（如果提供了新标题）
        if (req.getTitle() != null && !req.getTitle().isEmpty()) {
            if (isTitleDuplicate(conn, req.getTitle(), auth.getMid())) {
                return false;
            }
        }

        // 检查duration是否被改变
        if (isDurationChanged(conn, bv, req.getDuration())) {
            return false;
        }

        // 检查提供的信息是否与当前视频信息相同
        if (isInfoSameAsCurrent(conn, bv, req)) {
            return false;
        }

        return true;
    }

    private boolean isDurationChanged(Connection conn, String bv, float newDuration) throws SQLException {
        String sql = "SELECT duration FROM videos WHERE bv = ?";
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, bv);
            ResultSet rs = stmt.executeQuery();
            if (rs.next()) {
                float currentDuration = rs.getFloat("duration");
                return currentDuration != newDuration;
            }
        }
        return true; // 如果无法获取当前时长，假定已更改
    }

    private boolean isInfoSameAsCurrent(Connection conn, String bv, PostVideoReq req) throws SQLException {
        String sql = "SELECT title, description, publicTime FROM videos WHERE bv = ?";
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, bv);
            ResultSet rs = stmt.executeQuery();
            if (rs.next()) {
                String currentTitle = rs.getString("title");
                String currentDescription = rs.getString("description");
                Timestamp currentPublicTime = rs.getTimestamp("publicTime");

                boolean isTitleSame = currentTitle.equals(req.getTitle());
                boolean isDescriptionSame = (currentDescription == null && req.getDescription() == null) ||
                        (currentDescription != null && currentDescription.equals(req.getDescription()));
                boolean isPublicTimeSame = (currentPublicTime == null && req.getPublicTime() == null) ||
                        (currentPublicTime != null && currentPublicTime.equals(req.getPublicTime()));

                return isTitleSame && isDescriptionSame && isPublicTimeSame;
            }
        }
        return false;
    }


    @Override
    public List<String> searchVideo(AuthInfo auth, String keywords, int pageSize, int pageNum) {
        if(checkMid(auth)){
            auth.setMid(getMid(auth));
        }

        if (!isValidAuth(auth) || keywords == null || keywords.isEmpty() || pageSize <= 0 || pageNum <= 0) {
            return null;
        }

        List<String> keywordList = Arrays.asList(keywords.toLowerCase().split("\\s+"));
        String keywordMatchCase = buildKeywordMatchCase(keywordList);

        try (Connection conn = dataSource.getConnection()) {
            String visibilityCondition = buildVisibilityCondition(conn, auth);

            String sql = "SELECT v.bv, (" + keywordMatchCase + ") AS relevance, " +
                    "(SELECT COUNT(*) FROM view WHERE view.bv = v.bv) AS viewCount " +
                    "FROM videos v " +
                    "LEFT JOIN users u ON v.ownerMid = u.mid " +
                    "WHERE (" + keywordMatchCase + ") > 0 " +
                    "AND " + visibilityCondition + " " +
                    "ORDER BY relevance DESC, viewCount DESC " +
                    "LIMIT ? OFFSET ?";

            try (PreparedStatement stmt = conn.prepareStatement(sql)) {
                stmt.setInt(1, pageSize);
                stmt.setInt(2, (pageNum - 1) * pageSize);

                ResultSet rs = stmt.executeQuery();
                List<String> videoBvs = new ArrayList<>();
                while (rs.next()) {
                    videoBvs.add(rs.getString("bv"));
                }
                return videoBvs;
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private String buildVisibilityCondition(Connection conn, AuthInfo auth) throws SQLException {
        boolean isSuperuser = isSuperUser(conn, auth.getMid());
        if (isSuperuser) {
            return "1 = 1"; // 超级用户可以看到所有视频
        } else {
            return "(v.reviewtime IS NOT NULL AND v.publictime <= CURRENT_TIMESTAMP) OR v.ownerMid = " + auth.getMid();
        }
    }


    private String buildKeywordMatchCase(List<String> keywords) {
        StringBuilder caseBuilder = new StringBuilder();
        for (String keyword : keywords) {
            // 转义正则表达式的特殊字符
            String escapedKeyword = escapeRegexSpecialChars(keyword);

            if (caseBuilder.length() > 0) {
                caseBuilder.append(" + ");
            }
            // 使用正则表达式计算每个关键词的出现次数
            caseBuilder.append("(SELECT COUNT(*) FROM regexp_matches(lower(title), lower('").append(escapedKeyword).append("'), 'g')) ")
                    .append("+ (SELECT COUNT(*) FROM regexp_matches(lower(description), lower('").append(escapedKeyword).append("'), 'g')) ")
                    .append("+ (SELECT COUNT(*) FROM regexp_matches(lower(u.name), lower('").append(escapedKeyword).append("'), 'g')) ");
        }
        return caseBuilder.toString();
    }

    private String escapeRegexSpecialChars(String keyword) {
        // 正则表达式特殊字符列表
        String[] specialChars = new String[] { "\\", ".", "[", "]", "{", "}", "(", ")", "*", "+", "?", "^", "$", "|" };
        for (String specialChar : specialChars) {
            keyword = keyword.replace(specialChar, "\\" + specialChar);
        }
        return keyword;
    }





    @Override
    public double getAverageViewRate(String bv) {

        try(Connection conn=dataSource.getConnection()) {
            if(!doesVideoExist(conn,bv)){
                return -1;
            }
            float duration=getVideoDuration(conn,bv);
            if(duration<0){
                return -1;
            }
            String sql = "SELECT SUM(viewtime) as totalViewTime, COUNT(*) as viewCount FROM view WHERE bv = ?";
            try(PreparedStatement stmt = conn.prepareStatement(sql)){
                stmt.setString(1,bv);
                ResultSet rs = stmt.executeQuery();
                if(rs.next()){
                    double totalViewTime = rs.getDouble("totalviewtime");
                    int viewCount = rs.getInt("viewcount");
                    if(viewCount==0){
                        conn.close();
                        return 0;
                    }
                    conn.close();
                    return totalViewTime / (duration* viewCount);
                }
            }
        }catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return -1;
    }
    private float getVideoDuration(Connection conn, String bv) throws SQLException {
        String sql = "SELECT duration FROM videos WHERE bv = ?";
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, bv);
            ResultSet rs = stmt.executeQuery();
            if (rs.next()) {
                return rs.getFloat("duration");
            }
        }
        return -1; // 视频不存在或获取时长失败
    }

    @Override
    public Set<Integer> getHotspot(String bv) {
        Set<Integer> hotspots=new HashSet<>();
        try(Connection conn=dataSource.getConnection()){
            if(!doesVideoExist(conn,bv)){
                return null;
            }
            String sql = "SELECT FLOOR(time / 10) AS chunk, COUNT(*) AS danmuCount " +
                    "FROM danmus WHERE bv = ? " +
                    "GROUP BY chunk " +
                    "ORDER BY danmuCount DESC";
            try(PreparedStatement stmt = conn.prepareStatement(sql)){
                stmt.setString(1,bv);
                ResultSet rs = stmt.executeQuery();
                int maxCount = 0;
                while (rs.next()){
                    int chunkIndex= rs.getInt("chunk");
                    int danmuCount = rs.getInt("danmucount");
                    if(hotspots.isEmpty()||danmuCount==maxCount){
                        hotspots.add(chunkIndex);
                        maxCount=danmuCount;
                    }else {
                        break;
                    }
                }
            }
        }catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return hotspots;
    }

    @Override
    public boolean reviewVideo(AuthInfo auth, String bv) {
        try(Connection conn=dataSource.getConnection()){
            if(checkMid(auth)){
                auth.setMid(getMid(auth));
            }

            if(!isValidAuth(auth)){
                return false;
            }
            if(!doesVideoExist(conn,bv)){
                return false;
            }
            if(isOwnerOfVideo(conn,auth.getMid(),bv)){
                return false;
            }
            if(isVideoAlreadyReviewed(conn,bv)){
                return false;
            }
            return performReview(conn,bv);
        }catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
    private boolean performReview(Connection conn, String bv) throws SQLException {
        String sql = "UPDATE videos SET reviewtime = ? WHERE bv = ?";
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setTimestamp(1, new Timestamp(System.currentTimeMillis()));
            stmt.setString(2, bv);
            return stmt.executeUpdate() > 0;
        }
    }

    private boolean isVideoAlreadyReviewed(Connection conn, String bv) throws SQLException {
        String sql="select reviewtime from videos where bv=?";
        try(PreparedStatement stmt=conn.prepareStatement(sql)){
            stmt.setString(1,bv);
            ResultSet rs=stmt.executeQuery();
            if(rs.next()){
                Timestamp reviewTime=rs.getTimestamp("reviewtime");
                if(reviewTime==null){
                    return false;
                }else {
                    return true;
                }
            }else {
                return false;
            }
        }
    }


    @Override
    public boolean coinVideo(AuthInfo auth, String bv) {
        try (Connection conn = dataSource.getConnection()) {
            if(checkMid(auth)){
                auth.setMid(getMid(auth));
            }

            if (!isValidAuth(auth)) {
                return false; // 用户认证无效
            }

            if (!doesVideoExist(conn, bv)) {
                return false; // 视频不存在
            }

            if (!canUserSearchVideo(conn, auth, bv)) {
                return false; // 用户不能搜索此视频或者是视频的所有者
            }

            if (!hasCoinsAndNotDonated(conn, auth.getMid(), bv)) {
                return false; // 用户没有币或已对该视频投过币
            }
            // 执行投币操作
            return performCoinOperation(conn, auth.getMid(), bv);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private boolean canUserSearchVideo(Connection conn, AuthInfo auth, String bv) throws SQLException {
        String sql = "SELECT ownermid, reviewtime, publictime FROM videos WHERE bv = ?";
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, bv);
            ResultSet rs = stmt.executeQuery();

            if (!rs.next()) {
                return false; // 视频不存在
            }

            long ownerMid = rs.getLong("ownermid");
            Timestamp reviewTime = rs.getTimestamp("reviewtime");
            Timestamp publicTime = rs.getTimestamp("publictime");

            // 判断用户是否可以查看该视频
            boolean isOwner = ownerMid == auth.getMid();
            boolean isSuperUser = isSuperUser(conn, auth.getMid());
            boolean isReviewed = reviewTime != null && !reviewTime.after(new Timestamp(System.currentTimeMillis()));
            boolean isPublished = publicTime != null && !publicTime.after(new Timestamp(System.currentTimeMillis()));

            return isSuperUser || isOwner || (isReviewed && isPublished);
        }
    }

    private boolean hasCoinsAndNotDonated(Connection conn, long mid, String bv) throws SQLException {
        String sqlCoins = "SELECT coin FROM users WHERE mid = ?";
        try (PreparedStatement stmtCoins = conn.prepareStatement(sqlCoins)) {
            stmtCoins.setLong(1, mid);
            ResultSet rsCoins = stmtCoins.executeQuery();
            if (rsCoins.next() && rsCoins.getInt("coin") > 0) {
                // 用户有币，接下来检查是否已对该视频投过币
                String sqlDonated = "SELECT COUNT(*) FROM coin WHERE bv = ? AND mid = ?";
                try (PreparedStatement stmtDonated = conn.prepareStatement(sqlDonated)) {
                    stmtDonated.setString(1, bv);
                    stmtDonated.setLong(2, mid);
                    ResultSet rsDonated = stmtDonated.executeQuery();
                    if (rsDonated.next() && rsDonated.getInt(1) == 0) {
                        return true; // 用户有币且未对该视频投过币
                    }
                }
            }
        }
        return false; // 用户没有币或已对该视频投过币
    }

    private boolean performCoinOperation(Connection conn, long mid, String bv) throws SQLException {
        conn.setAutoCommit(false);

        try {
            // 记录投币操作
            String sqlInsertCoin = "INSERT INTO coin (bv, mid) VALUES (?, ?)";
            try (PreparedStatement stmtInsertCoin = conn.prepareStatement(sqlInsertCoin)) {
                stmtInsertCoin.setString(1, bv);
                stmtInsertCoin.setLong(2, mid);
                int insertedRows = stmtInsertCoin.executeUpdate();
                if (insertedRows == 0) {
                    conn.rollback(); // 如果插入失败，回滚事务
                    return false;
                }
            }

            // 减少用户币数
            String sqlUpdateUser = "UPDATE users SET coin = coin - 1 WHERE mid = ?";
            try (PreparedStatement stmtUpdateUser = conn.prepareStatement(sqlUpdateUser)) {
                stmtUpdateUser.setLong(1, mid);
                int updatedRows = stmtUpdateUser.executeUpdate();
                if (updatedRows == 0) {
                    conn.rollback(); // 如果更新失败，回滚事务
                    return false;
                }
            }

            conn.commit();
            return true;
        } catch (SQLException e) {
            conn.rollback();
            throw e;
        } finally {
            conn.setAutoCommit(true);
        }
    }



    @Override
    public boolean likeVideo(AuthInfo auth, String bv) {
        try (Connection conn = dataSource.getConnection()) {

            if (!isValidAuth(auth)) {
                return false; // 用户认证无效
            }

            if(checkMid(auth)){
                auth.setMid(getMid(auth));
                if(auth.getMid()==-1){
                    return false;
                }
            }



            if (!doesVideoExist(conn, bv)) {
                return false; // 视频不存在
            }

            if (!canUserSearchVideo(conn, auth, bv)) {
                return false; // 用户不能搜索此视频或者是视频的所有者
            }

            // 检查用户是否已经对视频点赞
            boolean alreadyLiked = checkIfAlreadyLiked(conn, auth.getMid(), bv);
            if (alreadyLiked) {
                // 如果已点赞，执行取消点赞操作
                removeLike(conn, auth.getMid(), bv);
                return false;
            } else {
                // 如果未点赞，执行点赞操作
                addLike(conn, auth.getMid(), bv);
                return true;
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private boolean checkIfAlreadyLiked(Connection conn, long mid, String bv) throws SQLException {
        String sql = "SELECT COUNT(*) FROM videolike WHERE bv = ? AND mid = ?";
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, bv);
            stmt.setLong(2, mid);
            ResultSet rs = stmt.executeQuery();
            if (rs.next()) {
                if(rs.getInt(1)>0){
                    return true;
                }else {
                    return false;
                }
            }else {
                return false;
            }
        }

    }

    private void addLike(Connection conn, long mid, String bv) throws SQLException {
        String sql = "INSERT INTO videolike (bv, mid) VALUES (?, ?)";
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, bv);
            stmt.setLong(2, mid);
            stmt.executeUpdate();
        }
    }

    private void removeLike(Connection conn, long mid, String bv) throws SQLException {
        String sql = "DELETE FROM videolike WHERE bv = ? AND mid = ?";
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, bv);
            stmt.setLong(2, mid);
            stmt.executeUpdate();
        }
    }


    @Override
    public boolean collectVideo(AuthInfo auth, String bv) {
        try (Connection conn = dataSource.getConnection()) {
            if(checkMid(auth)){
                auth.setMid(getMid(auth));
                if(auth.getMid()==-1){
                    return false;
                }
            }

            if (!isValidAuth(auth)) {
                return false; // 用户认证无效
            }

            if (!doesVideoExist(conn, bv)) {
                return false; // 视频不存在
            }

            if (!canUserSearchVideo(conn, auth, bv)) {
                return false; // 用户不能搜索此视频或者是视频的所有者
            }

            // 检查用户是否已经对视频收藏
            boolean alreadyCollected = checkIfAlreadyCollected(conn, auth.getMid(), bv);
            if (alreadyCollected) {
                // 如果已收藏，执行取消收藏操作
                removeCollection(conn, auth.getMid(), bv);
                return false;
            } else {
                // 如果未收藏，执行收藏操作
                addCollection(conn, auth.getMid(), bv);
                return true;
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private boolean checkIfAlreadyCollected(Connection conn, long mid, String bv) throws SQLException {
        String sql = "SELECT COUNT(*) FROM favorite WHERE bv = ? AND mid = ?";
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, bv);
            stmt.setLong(2, mid);
            ResultSet rs = stmt.executeQuery();
            if (rs.next()) {
                if(rs.getInt(1)>0){
                    return true;
                }else {
                    return false;
                }
            }else {
                return false;
            }
        }

    }

    private void addCollection(Connection conn, long mid, String bv) throws SQLException {
        // 实现添加收藏的逻辑
        String sql = "INSERT INTO favorite (bv, mid) VALUES (?, ?)";
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, bv);
            stmt.setLong(2, mid);
            stmt.executeUpdate();
        }
    }

    private void removeCollection(Connection conn, long mid, String bv) throws SQLException {
        // 实现取消收藏的逻辑
        String sql = "DELETE FROM favorite WHERE bv = ? AND mid = ?";
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, bv);
            stmt.setLong(2, mid);
            stmt.executeUpdate();
        }
    }

    public boolean checkMid(AuthInfo auth){
        return auth.getMid() <= 0;
    }

    public long getMid(AuthInfo auth){
        try (Connection conn = dataSource.getConnection()) {
            if (auth.getQq() != null && !auth.getQq().isEmpty()) {
                return getMidFromIdentifier(conn, "qq", auth.getQq());
            }
            if (auth.getWechat() != null && !auth.getWechat().isEmpty()) {
                return getMidFromIdentifier(conn, "wechat", auth.getWechat());
            }
            return -1; // 如果没有提供有效的 QQ 或 WeChat，返回 -1
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private long getMidFromIdentifier(Connection conn, String fieldName, String identifier) throws SQLException {
        String sql = "SELECT mid FROM users WHERE " + fieldName + " = ?";
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, identifier);
            ResultSet rs = stmt.executeQuery();
            if (rs.next()) {
                return rs.getLong("mid");
            }
        }
        return -1; // 如果没有找到对应的用户，返回 -1
    }
}
