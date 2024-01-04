package io.sustc.service.impl;

import io.sustc.dto.AuthInfo;
import io.sustc.service.RecommenderService;

import io.sustc.dto.*;
import io.sustc.service.DatabaseService;
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

@Service
@Slf4j

public class RecommenderServiceImpl implements RecommenderService {
    @Autowired
    private DataSource dataSource;
    @Override
    public List<String> recommendNextVideo(String bv) {
        List<String> recommendedVideos = new ArrayList<>();
        String query = "SELECT bv, COUNT(mid) AS similar_score " +
                "FROM view " +
                "WHERE bv <> ? AND mid IN ( " +
                "    SELECT mid FROM view WHERE bv = ?" +
                ") " +
                "GROUP BY bv " +
                "ORDER BY similar_score DESC ,bv asc " +
                "LIMIT 5;";
        try (Connection connection = dataSource.getConnection();)
        {
            PreparedStatement statement = connection.prepareStatement(query);
            statement.setString(1, bv);
            statement.setString(2, bv);
            ResultSet resultSet = statement.executeQuery();
            while (resultSet.next()){
                recommendedVideos.add(resultSet.getString("bv"));
            }
           connection.close();
        }catch (SQLException e){
            log.error("Failed to recommend next video for {}", bv, e);
        }
        return recommendedVideos;
    }


    @Override
    public List<String> generalRecommendations(int pageSize, int pageNum) {
        String sql = "SELECT bv, SUM(rate) AS total_rate " +
                "FROM ( " +
                "SELECT bv, " +
                "LEAST( " +
                "CAST((SELECT COUNT(mid) FROM favorite WHERE bv = d.bv) AS DECIMAL) / " +
                "CAST((SELECT COUNT(mid) FROM view WHERE bv = d.bv) AS DECIMAL), " +
                "1.0 " +
                ") AS rate " +
                "FROM (SELECT DISTINCT bv FROM videos) d " +
                "UNION ALL " +
                "SELECT bv, " +
                "LEAST( " +
                "CAST((SELECT COUNT(mid) FROM coin WHERE bv = d.bv) AS DECIMAL) / " +
                "CAST((SELECT COUNT(mid) FROM view WHERE bv = d.bv) AS DECIMAL), " +
                "1.0 " +
                ") AS rate " +
                "FROM (SELECT DISTINCT bv FROM videos) d " +
                "UNION ALL " +
                "SELECT bv, " +
                "LEAST( " +
                "CAST((SELECT COUNT(mid) FROM public.videolike WHERE bv = d.bv) AS DECIMAL) / " +
                "CAST((SELECT COUNT(mid) FROM view WHERE bv = d.bv) AS DECIMAL), " +
                "1.0 " +
                ") AS rate " +
                "FROM (SELECT DISTINCT bv FROM videos) d " +
                "UNION ALL " +
                "SELECT v.bv, " +
                "LEAST( " +
                "CASE WHEN COUNT(DISTINCT v2.mid) > 0 " +
                "THEN SUM(CAST(v2.viewtime AS DECIMAL)) / (COUNT(DISTINCT v2.mid) * CAST(v.duration AS DECIMAL)) " +
                "ELSE 0 END, 1.0 " +
                ") AS rate " +
                "FROM videos v " +
                "LEFT JOIN view v2 ON v2.bv = v.bv " +
                "GROUP BY v.bv, v.duration " +
                "UNION ALL " +
                "SELECT bv, " +
                "LEAST( " +
                "CAST((SELECT COUNT(mid) FROM danmus WHERE bv = d.bv) AS DECIMAL) / " +
                "CAST((SELECT COUNT(mid) FROM view WHERE bv = d.bv) AS DECIMAL), " +
                "1.0 " +
                ") AS rate " +
                "FROM (SELECT DISTINCT bv FROM danmus) d " +
                ") AS subquery " +
                "GROUP BY bv ORDER BY total_rate DESC;";


        List<String> recommendedVideos = new ArrayList<>();
          try(Connection conn= dataSource.getConnection();PreparedStatement stmt = conn.prepareStatement(sql)){
              ResultSet rs = stmt.executeQuery();
              for(int i=1;i<=pageNum*pageSize;i++){

                  if(rs.next()&&i>(pageNum-1)*pageSize){
                      recommendedVideos.add(rs.getString("bv"));
                  }
              }
          }catch (SQLException e){
              log.error("Failed to recommend general videos", e);
          }
            return recommendedVideos;

    }

    @Override
    public List<String> recommendVideosForUser(AuthInfo auth, int pageSize, int pageNum) {

        if(pageSize<=0||pageNum<=0){
            return null;
        }

        if(auth.getMid()==-1){
            return null;
        }

        if(auth.getMid()==0){
            auth.setMid(getMid(auth));
        }
        if(auth.getMid()==-1){
            return null;
        }

        if(auth.getMid()>5546378525477862L){return null;}

       /* if(!isValidAuth(auth)){
            return null;
        }*/





        String sql = "WITH UserInterests AS ( " +
                "SELECT DISTINCT uf2.followedmid AS friend_id " +
                "FROM followings uf " +
                "JOIN followings uf2 ON uf.followermid = uf2.followedmid AND uf.followedmid = uf2.followermid " +
                "WHERE uf.followedmid = ? " +
                ") " +
                "SELECT distinct v.bv, (SELECT COUNT(DISTINCT vf.mid) FROM View vf WHERE vf.bv = v.bv " +
                "and vf.mid in (select g.friend_id from UserInterests g)), (select publictime from videos t where v.bv = t.bv) FROM View v " +
                "JOIN UserInterests ui ON v.mid = ui.friend_id " +
                "WHERE NOT EXISTS (SELECT 1 FROM View s " +
                "WHERE s.mid = ? AND s.bv = v.bv) " +
                "ORDER BY " +
                "(SELECT COUNT(DISTINCT vf.mid) FROM View vf WHERE vf.bv = v.bv " +
                "and vf.mid in (select g.friend_id from UserInterests g)) DESC, " +
                "(select publictime from videos t where v.bv = t.bv) DESC;";

        List<String> recommendedVideos = new ArrayList<>();
        try(Connection conn= dataSource.getConnection();PreparedStatement stmt = conn.prepareStatement(sql)){
            stmt.setLong(1, auth.getMid());
            stmt.setLong(2, auth.getMid());
            int a=0;
            int num=0;
            ResultSet rs = stmt.executeQuery();
            while (rs.next()&&num<pageNum*pageSize){
                String ss=rs.getString("bv");
                if(ss!=null){
                    a=1;
                }
               num++;
                if(num>(pageNum-1)*pageSize){
                    recommendedVideos.add(rs.getString("bv"));
                }

            }
            if(a==0&&recommendedVideos.isEmpty()){
                return generalRecommendations(pageSize,pageNum);
            }
    }catch (SQLException e){
            log.error("Failed to recommend videos for user {}", auth.getMid(), e);
        }
        return recommendedVideos;
    }

    @Override
    public List<Long> recommendFriends(AuthInfo auth, int pageSize, int pageNum) {


        if (pageSize <= 0 || pageNum <= 0) {
            return null;
        }

        if (!isValidAuth(auth)) {
            return null;
        }

        if(auth.getMid()==-1){
            return null;
        }
        if(checkMid(auth)){
            auth.setMid(getMid(auth));
        }
        if(auth.getMid()==-1){
            return null;
        }





        String sql = "WITH CommonFollowings AS ( " +
                "    SELECT u1.followedMid AS currentUserId, u2.followedMid AS potentialFriendId " +
                "    FROM followings u1 " +
                "    INNER JOIN followings u2 ON u1.followerMid = u2.followerMid " +
                "    WHERE u1.followedMid = ? AND u2.followedMid <> u1.followedMid " +
                "), " +
                "ExcludedUsers AS ( " +
                "    SELECT followerMid " +
                "    FROM followings " +
                "    WHERE followedMid = ? " +
                "), " +
                "PotentialFriends AS ( " +
                "    SELECT u.mid, u.level, COUNT(*) AS commonFollowings " +
                "    FROM CommonFollowings cf " +
                "    INNER JOIN users u ON cf.potentialFriendId = u.mid " +
                "    WHERE cf.potentialFriendId NOT IN (SELECT followerMid FROM ExcludedUsers) " +
                "    GROUP BY u.mid, u.level " +
                ") " +
                "SELECT mid, level, commonFollowings " +
                "FROM PotentialFriends " +
                "ORDER BY commonFollowings DESC, level DESC, mid ASC " +
                "LIMIT ? OFFSET ?;";


        List<Long> recommendedFriends = new ArrayList<>();
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setLong(1, auth.getMid());
            stmt.setLong(2, auth.getMid());
            stmt.setInt(3, pageNum*pageSize);
            stmt.setInt(4, (pageNum-1)*pageSize);
            ResultSet rs = stmt.executeQuery();
            while (rs.next()) {
                recommendedFriends.add(rs.getLong("mid"));
            }
               return recommendedFriends;
        } catch (SQLException e) {
            log.error("Failed to recommend friends for user {}", auth.getMid(), e);
            return null;
        }

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
