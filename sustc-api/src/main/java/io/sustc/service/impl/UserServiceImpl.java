package io.sustc.service.impl;

import io.sustc.dto.AuthInfo;
import io.sustc.dto.RegisterUserReq;
import io.sustc.dto.UserInfoResp;
import io.sustc.service.UserService;
import io.sustc.dto.*;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.stereotype.Service;
import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

@Service
@Slf4j

public class UserServiceImpl implements UserService {
    @Autowired
    private DataSource dataSource;
    @Override
    public long register(RegisterUserReq req) {
        if(req.getPassword()==null||req.getPassword().isEmpty()||
                req.getName()==null||req.getName().isEmpty()||
        req.getSex()==null||req.getSex().toString().isEmpty())
        {
            return -1;
        }
        if(req.getBirthday()!=null){
            if(!isValidBirthday(req.getBirthday())){
                return -1;
            }
        }
        if(!(req.getQq()==null&&req.getWechat()==null)){
            if(!isQQorWechatExists(req.getQq(),req.getWechat())){
                return -1;
            }
        }

        String sqlFindMaxMid = "SELECT MAX(mid) FROM users";
        String sqlInsert = "INSERT INTO users (mid, password, qq, wechat, name, sex, birthday, sign) VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
        try(Connection conn = dataSource.getConnection();
        PreparedStatement stmtFindMaxMid = conn.prepareStatement(sqlFindMaxMid);
        PreparedStatement stmtInsert = conn.prepareStatement(sqlInsert)){

            ResultSet rs = stmtFindMaxMid.executeQuery();
            long newMid = rs.next() ? rs.getLong(1) + 1 : 1;
            stmtInsert.setLong(1, newMid);
            stmtInsert.setString(2, req.getPassword());
            stmtInsert.setString(3, req.getQq());
            stmtInsert.setString(4, req.getWechat());
            stmtInsert.setString(5, req.getName());
            stmtInsert.setString(6, req.getSex().toString());
            stmtInsert.setString(7,req.getBirthday());
            stmtInsert.setString(8,req.getSign());
            stmtInsert.executeUpdate();
            conn.close();
            return newMid;
        }catch (SQLException e){
            throw new RuntimeException(e);
        }

    }
    private boolean isValidBirthday(String birthday) {

        String[] parts = birthday.split("月");
        if (parts.length != 2 || !parts[1].endsWith("日")) {
            return false;
        }

        try {
            int month = Integer.parseInt(parts[0]);
            int day = Integer.parseInt(parts[1].replace("日", ""));

            if (month < 1 || month > 12) return false;
            if (day < 1 || day > 31) return false;

            if(month==2){
                if(day>29){
                    return false;
                }
            }
            if(month==4||month==6||month==9||month==11){
                if(day>30){
                    return false;
                }
            }

            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }


    private boolean isQQorWechatExists(String qq, String wechat) {
        String sql = "SELECT COUNT(*) FROM users WHERE qq = ?";
        String sql2 = "SELECT COUNT(*) FROM users WHERE wechat = ?";
        try (Connection conn = dataSource.getConnection();) {
            if(qq!=null){
                try (PreparedStatement stmt = conn.prepareStatement(sql)) {
                    stmt.setString(1, qq);
                    ResultSet rs = stmt.executeQuery();
                    if (rs.next()) {
                        if(rs.getInt(1)>0){
                            return false;
                        }
                    }
                }
            }
            if(wechat!=null){
                try (PreparedStatement stmt = conn.prepareStatement(sql2)) {
                    stmt.setString(1, wechat);
                    ResultSet rs = stmt.executeQuery();
                    if (rs.next()) {
                        if(rs.getInt(1)>0){
                            return false;
                        }
                    }
                }
            }
            return true;
        }catch (SQLException e){
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean deleteAccount(AuthInfo auth, long mid) {
        try(Connection conn = dataSource.getConnection()){
            conn.setAutoCommit(false);

            if(checkMid(auth)){
                auth.setMid(getMid(auth));
            }

            if(!doesUserExist(conn,mid)){
                conn.rollback();
                return false;
            }
            if(!isValidAuth(auth)){
                conn.rollback();
                return false;
            }

            if(!isAuthorizedToDelete(conn,auth,mid)){
                conn.rollback();
                return false;
            }
            deleteUserRelatedData(conn,mid);
            deleteUser(conn,mid);
            conn.commit();
            conn.close();
            return true;
        }catch (SQLException e){
            throw new RuntimeException(e);
        }

    }
    private void deleteUserRelatedData(Connection conn, long midToDelete) throws SQLException {
        // 删除用户发表的视频
        String sqlDeleteVideos = "DELETE FROM videos WHERE ownerMid = ?";
        try (PreparedStatement stmtDeleteVideos = conn.prepareStatement(sqlDeleteVideos)) {
            stmtDeleteVideos.setLong(1, midToDelete);
            stmtDeleteVideos.executeUpdate();
        }

        // 删除用户发送的弹幕
        String sqlDeleteDanmus = "DELETE FROM danmus WHERE mid = ?";
        try (PreparedStatement stmtDeleteDanmus = conn.prepareStatement(sqlDeleteDanmus)) {
            stmtDeleteDanmus.setLong(1, midToDelete);
            stmtDeleteDanmus.executeUpdate();
        }

    }


    private void deleteUser(Connection conn, long midToDelete) throws SQLException {
        String sqlDeleteUser = "DELETE FROM users WHERE mid = ?";
        try (PreparedStatement stmtDeleteUser = conn.prepareStatement(sqlDeleteUser)) {
            stmtDeleteUser.setLong(1, midToDelete);
            stmtDeleteUser.executeUpdate();
        }
    }

    private boolean doesUserExist(Connection conn, long mid) throws SQLException {
        String sql = "SELECT COUNT(*) FROM users WHERE mid = ?";
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setLong(1, mid);
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
    private boolean isAuthorizedToDelete(Connection conn, AuthInfo auth, long midToDelete) throws SQLException {
        long userMid = auth.getMid();

        // 如果 auth 中没有提供 mid，尝试通过 qq 或 wechat 获取
        if (userMid <= 0) {
            userMid = getMidFromAuth(conn, auth);
            if (userMid <= 0) {
                return false; // 无法获取有效的 mid
            }
        }
      String sql="select identity from users where mid = ?";
        String sql2="select identity from users where mid = ?";
        try(PreparedStatement stmt = conn.prepareStatement(sql);
            PreparedStatement stmt2 = conn.prepareStatement(sql2)){
            stmt.setLong(1,userMid);
            ResultSet rs = stmt.executeQuery();
            stmt2.setLong(1,midToDelete);
            ResultSet rs2 = stmt2.executeQuery();
            if(rs.next()&&rs2.next()){
            if(Objects.equals(rs.getString("identity"), "SUPERUSER")) {
                if(rs2.getString("identity")==null){
                    return true;
                }
                if(Objects.equals(rs2.getString("identity"), "SUPERUSER")){
                    if(userMid==midToDelete){
                        return true;
                    }else {
                        return false;
                    }
                }else {
                    return true;
                }
            }else {
                if(userMid==midToDelete){
                    return true;
                }
            }
            }
            return false;
        }

    }


    private long getMidFromAuth(Connection conn, AuthInfo auth) throws SQLException {
        String sql = "SELECT mid FROM users WHERE qq = ? OR wechat = ?";
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, auth.getQq());
            stmt.setString(2, auth.getWechat());
            ResultSet rs = stmt.executeQuery();

            if (rs.next()) {
                return rs.getLong("mid");
            }
        }

        return -1;
    }

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
            }
            return false;
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
    public boolean follow(AuthInfo auth, long followeeMid) {
        try (Connection conn = dataSource.getConnection()) {

            if(checkMid(auth)){
                auth.setMid(getMid(auth));
            }

            if (!isValidAuth(auth) || !doesUserExist(conn, followeeMid)) {
                return false;
            }


            long followerMid = auth.getMid();
            if(followerMid==followeeMid){
                return false;
            }
            if (isAlreadyFollowing(conn, followerMid, followeeMid)) {
                // 如果已经关注了，取消关注
                unfollowUser(conn, followerMid, followeeMid);
                conn.close();
                return false;
            } else {
                // 否则，关注用户
                followUser(conn, followerMid, followeeMid);
                conn.close();
                return true;
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private boolean isAlreadyFollowing(Connection conn, long followerMid, long followeeMid) throws SQLException {
        // 实现检查用户是否已关注的逻辑
        String sql= "SELECT COUNT(*) FROM followings WHERE followermid = ? AND followedmid = ?";
        try(PreparedStatement stmt = conn.prepareStatement(sql)){
            stmt.setLong(1,followerMid);
            stmt.setLong(2,followeeMid);
            ResultSet rs = stmt.executeQuery();
            if(rs.next()){
                return rs.getInt(1)>0;
            }
            return false;
        }catch (SQLException e){
            throw new RuntimeException(e);
        }
    }

    private void followUser(Connection conn, long followerMid, long followeeMid) throws SQLException {
        // 实现关注用户的逻辑
        String sql = "INSERT INTO followings (followedmid, followermid) VALUES (?, ?)";
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setLong(1, followeeMid);
            stmt.setLong(2, followerMid);
            stmt.executeUpdate();
        }catch (SQLException e){
            throw new RuntimeException(e);
        }
    }

    private void unfollowUser(Connection conn, long followerMid, long followeeMid) throws SQLException {
        // 实现取消关注用户的逻辑
        String sql = "DELETE FROM followings WHERE followedmid = ? AND followermid = ?";
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setLong(1, followeeMid);
            stmt.setLong(2, followerMid);
            stmt.executeUpdate();
    }
    catch (SQLException e){
        throw new RuntimeException(e);
        }
    }


    @Override
    public UserInfoResp getUserInfo(long mid) {
        try (Connection conn = dataSource.getConnection()) {
            if (!doesUserExist(conn, mid)) {
                return null;
            }
            UserInfoResp userInfo = new UserInfoResp();
            userInfo.setMid(mid);
            userInfo.setCoin(getUserCoin(conn, mid));
            userInfo.setFollowing(getUserFollowing(conn, mid));
            userInfo.setFollower(getUserFollowers(conn, mid));
            userInfo.setWatched(getUserWatchedVideos(conn, mid));
            userInfo.setLiked(getUserLikedVideos(conn, mid));
            userInfo.setCollected(getUserCollectedVideos(conn, mid));
            userInfo.setPosted(getUserPostedVideos(conn, mid));
            conn.close();
            return userInfo;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }


    private int getUserCoin(Connection conn, long mid) throws SQLException {
        String sql= "SELECT coin FROM users WHERE mid = ?";
        try(PreparedStatement stmt = conn.prepareStatement(sql)){
            stmt.setLong(1,mid);
            ResultSet rs = stmt.executeQuery();
            if(rs.next()){
                return rs.getInt("coin");
            }
            return 0;

       }catch (SQLException e){
            throw new RuntimeException(e);
        }
    }

    private long[] getUserFollowing(Connection conn, long mid) throws SQLException {
        List<Long> followingList = new ArrayList<>();
        String sql = "SELECT followermid FROM followings WHERE followedmid = ?";

        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setLong(1, mid);
            ResultSet rs = stmt.executeQuery();

            while (rs.next()) {
                followingList.add(rs.getLong("followermid"));
            }
        }

        long[] followingArray = new long[followingList.size()];
        for (int i = 0; i < followingList.size(); i++) {
            followingArray[i] = followingList.get(i);
        }

        return followingArray;
    }


    private long[] getUserFollowers(Connection conn, long mid) throws SQLException {
        List<Long> followersList = new ArrayList<>();
        String sql = "SELECT followedmid FROM followings WHERE followermid = ?";

      try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setLong(1, mid);
            ResultSet rs = stmt.executeQuery();

            while (rs.next()) {
                followersList.add(rs.getLong("followedmid"));
            }
        }
      Long[] followersArray = new Long[followersList.size()];
        followersList.toArray(followersArray);

        long[] followersArray2 = new long[followersArray.length];
        for (int i = 0; i < followersArray.length; i++) {
            followersArray2[i] = followersArray[i];
        }

        return followersArray2;
    }


    private String[] getUserWatchedVideos(Connection conn, long mid) throws SQLException {
        List<String> watchedVideosList = new ArrayList<>();
        String sql = "SELECT bv FROM view WHERE mid = ?";

        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setLong(1, mid);
            ResultSet rs = stmt.executeQuery();

            while (rs.next()) {
                watchedVideosList.add(rs.getString("bv"));
            }
        }

        String[] watchedVideosArray = new String[watchedVideosList.size()];
        watchedVideosList.toArray(watchedVideosArray);

        return watchedVideosArray;
    }


    private String[] getUserLikedVideos(Connection conn, long mid) throws SQLException {
        List<String> likedVideosList = new ArrayList<>();
        String sql = "SELECT bv FROM videolike WHERE mid = ?";

        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setLong(1, mid);
            ResultSet rs = stmt.executeQuery();

            while (rs.next()) {
                likedVideosList.add(rs.getString("bv"));
            }
        }

        String[] likedVideosArray = new String[likedVideosList.size()];
        likedVideosList.toArray(likedVideosArray);

        return likedVideosArray;
    }


    private String[] getUserCollectedVideos(Connection conn, long mid) throws SQLException {
        List<String> collectedVideosList = new ArrayList<>();
        String sql = "SELECT bv FROM favorite WHERE mid = ?";

        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setLong(1, mid);
            ResultSet rs = stmt.executeQuery();

            while (rs.next()) {
                collectedVideosList.add(rs.getString("bv"));
            }
        }

        String[] collectedVideosArray = new String[collectedVideosList.size()];
        collectedVideosList.toArray(collectedVideosArray);

        return collectedVideosArray;
    }

    private String[] getUserPostedVideos(Connection conn, long mid) throws SQLException {
        String sql = "SELECT bv FROM videos WHERE ownermid = ?";
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setLong(1, mid);
            ResultSet rs = stmt.executeQuery();

            List<String> postedVideosList = new ArrayList<>();
            while (rs.next()) {
                postedVideosList.add(rs.getString("bv"));
            }

            String[] postedVideosArray = new String[postedVideosList.size()];
            postedVideosList.toArray(postedVideosArray);

            return postedVideosArray;
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
