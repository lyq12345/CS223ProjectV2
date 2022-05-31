package cs223.group8;

import cs223.group8.entity.DataItem;
import cs223.group8.mapper.DataItemMapper;
import cs223.group8.session.GeneralSessionConfig;
import cs223.group8.session.LeaderSessionConfig;
import org.apache.ibatis.session.SqlSession;

public class Test {
    public static void main(String[] args) {
        GeneralSessionConfig.createNewSession("follower3");

    }
}
