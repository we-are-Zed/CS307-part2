package io.sustc.service.impl;

import io.sustc.dto.DanmuRecord;
import io.sustc.dto.UserRecord;
import io.sustc.dto.VideoRecord;
import io.sustc.service.DatabaseService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import io.sustc.dto.AuthInfo;
import io.sustc.service.DanmuService;
import org.springframework.stereotype.Service;
import javax.sql.DataSource;
import java.sql.*;

import java.util.ArrayList;
import java.util.List;

@Service
@Slf4j
public class DanmuServiceImpl implements DanmuService {
    @Autowired
    private DataSource dataSource;
    @Override
    public long sendDanmu(AuthInfo auth, String bv, String content, float time) {

        if(checkMid(auth)){
            auth.setMid(getMid(auth));
        }

        // 验证授权信息
        if (!isValidAuth(auth)) {
            return -1;
        }

        // 检查视频存在性和状态
        if (!isVideoAvailable(bv)) {
            return -1;
        }

        // 确认用户已观看视频
        if (!hasUserWatchedVideo(auth.getMid(), bv)) {
            return -1;
        }

        // 检查弹幕内容的有效性
        if (content == null) {
            return -1;
        }

       if(!isValidTime(auth,bv,time)){
            return -1;
        }

        // 处理弹幕发送
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement("INSERT INTO danmus (bv, mid, time, content, posttime) VALUES (?, ?, ?, ?, ?)", Statement.RETURN_GENERATED_KEYS)) {

            stmt.setString(1, bv);
            stmt.setLong(2, auth.getMid());
            stmt.setFloat(3, time);
            stmt.setString(4, content);
            stmt.setTimestamp(5, new Timestamp(System.currentTimeMillis()));
            stmt.executeUpdate();

            try (ResultSet generatedKeys = stmt.getGeneratedKeys()) {
                if (generatedKeys.next()) {
                    return generatedKeys.getLong(1);
                }
            }

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        return -1;
    }
    private boolean isValidTime(AuthInfo auth,String bv,float time){
        String sql="SELECT duration FROM videos WHERE bv = ?";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {

            stmt.setString(1, bv);
            ResultSet rs = stmt.executeQuery();

            if (rs.next()) {
                if(time>rs.getFloat("duration")){
                    return false;
                }
            }
            return true;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
    private boolean doesMidExist(long mid) {
        String sql = "SELECT COUNT(*) FROM users WHERE mid = ?";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {

            stmt.setLong(1, mid);
            ResultSet rs = stmt.executeQuery();

            if (rs.next() && rs.getInt(1) > 0) {
                return true;
            }
            return false;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    // 示例辅助方法
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


    private boolean isVideoAvailable(String bv) {
        String sql = "SELECT reviewtime, publictime FROM videos WHERE bv = ?";

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {

            stmt.setString(1, bv);
            ResultSet rs = stmt.executeQuery();

            if (!rs.next()) {
                return false;
            }

            Timestamp reviewTime = rs.getTimestamp("reviewtime");
            Timestamp publicTime = rs.getTimestamp("publictime");
            Timestamp currentTime = new Timestamp(System.currentTimeMillis());

            // 检查视频是否被审核且已发布
            if (reviewTime != null && publicTime != null &&
                    reviewTime.before(currentTime) && publicTime.before(currentTime)) {
                return true;
            }
            conn.close();
            return false;

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }


    private boolean hasUserWatchedVideo(long mid, String bv) {
        String sql = "SELECT COUNT(*) FROM view WHERE bv = ? AND mid = ?";

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {

            stmt.setString(1, bv);
            stmt.setLong(2, mid);
            ResultSet rs = stmt.executeQuery();

            if (rs.next() && rs.getInt(1) > 0) {
                return true;
            }
             conn.close();
            return false;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }



    @Override
    public List<Long> displayDanmu(String bv, float timeStart, float timeEnd, boolean filter) {

        if (!isVideoAvailable(bv)) {
            return null;
        }

        Float videoDuration = getVideoDuration(bv);
        if (videoDuration == null || timeStart < 0 || timeEnd < 0 || timeStart > timeEnd || timeEnd > videoDuration) {
            return null;
        }

        String sql = filter ?
                "SELECT id FROM danmus WHERE bv = ? AND time >= ? AND time <= ? GROUP BY content, id ORDER BY MIN(postTime), time" :
                "SELECT id FROM danmus WHERE bv = ? AND time >= ? AND time <= ? ORDER BY time";

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {

            stmt.setString(1, bv);
            stmt.setFloat(2, timeStart);
            stmt.setFloat(3, timeEnd);
            ResultSet rs = stmt.executeQuery();

            List<Long> danmuIds = new ArrayList<>();
            while (rs.next()) {
                danmuIds.add(rs.getLong("id"));
            }
            conn.close();
            return danmuIds;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private Float getVideoDuration(String bv) {
        String sql = "SELECT duration FROM videos WHERE bv = ?";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {

            stmt.setString(1, bv);
            ResultSet rs = stmt.executeQuery();

            if (rs.next()) {
                return rs.getFloat("duration");
            }

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return null;
    }


    @Override
    public boolean likeDanmu(AuthInfo auth, long id) {

        if(checkMid(auth)){
            auth.setMid(getMid(auth));
        }
        // 验证用户认证信息
        if (!isValidAuth(auth)) {
            return false;
        }

        // 检查弹幕存在性
        if (!isDanmuExist(id)) {
            return false;
        }

        // 检查用户是否已观看视频
        String bv = getBvByDanmuId(id);
        if (!hasUserWatchedVideo(auth.getMid(), bv)) {
            return false;
        }

        // 处理点赞操作
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmtCheck = conn.prepareStatement("SELECT COUNT(*) FROM danmulike WHERE danmuid = ? AND mid = ?");
             PreparedStatement stmtInsert = conn.prepareStatement("INSERT INTO danmulike (danmuid, mid) VALUES (?, ?)");
             PreparedStatement stmtDelete = conn.prepareStatement("DELETE FROM danmulike WHERE danmuid = ? AND mid = ?")) {

            // 检查用户是否已点赞
            stmtCheck.setLong(1, id);
            stmtCheck.setLong(2, auth.getMid());
            ResultSet rs = stmtCheck.executeQuery();
            if (rs.next() && rs.getInt(1) > 0) {
                // 如果已点赞，取消点赞
                stmtDelete.setLong(1, id);
                stmtDelete.setLong(2, auth.getMid());
                stmtDelete.executeUpdate();
                conn.close();
                return false;
            } else {
                // 如果未点赞，添加点赞
                stmtInsert.setLong(1, id);
                stmtInsert.setLong(2, auth.getMid());
                stmtInsert.executeUpdate();
                conn.close();
                return true;
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    // 示例辅助方法
    private boolean isDanmuExist(long id) {
        String sql = "SELECT COUNT(*) FROM danmus WHERE id = ?";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {

            stmt.setLong(1, id);
            ResultSet rs = stmt.executeQuery();

            if (rs.next() && rs.getInt(1) > 0) {
                return true;
            }
            return false;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private String getBvByDanmuId(long id) {
        String sql = "SELECT bv FROM danmus WHERE id = ?";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {

            stmt.setLong(1, id);
            ResultSet rs = stmt.executeQuery();

            if (rs.next()) {
                return rs.getString("bv");
            }
            return null;
        } catch (SQLException e) {
            throw new RuntimeException(e);
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
