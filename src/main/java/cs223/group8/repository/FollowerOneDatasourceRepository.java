package cs223.group8.repository;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import cs223.group8.entity.DataItem;
import cs223.group8.mapper.DataItemMapper;
import cs223.group8.session.LeaderSessionConfig;
import org.apache.ibatis.session.SqlSession;

public class FollowerOneDatasourceRepository {
    public Integer readItemValue(String key) {
        SqlSession session = LeaderSessionConfig.sqlSession;
        DataItemMapper mapper = session.getMapper(DataItemMapper.class);
        QueryWrapper<DataItem> queryWrapper = new QueryWrapper<>();
        queryWrapper.eq("key", key);
        DataItem item = mapper.selectOne(queryWrapper);
        return item.getValue();
    }

    public void writeItem(String key, Integer value) {
        SqlSession session = LeaderSessionConfig.sqlSession;
        DataItemMapper mapper = session.getMapper(DataItemMapper.class);
        DataItem item = new DataItem(key, value);
        QueryWrapper<DataItem> queryWrapper = new QueryWrapper<>();
        queryWrapper.eq("key", key);
        if(null == mapper.selectOne(queryWrapper)){
            mapper.insert(item);
        }else{
            mapper.update(item, queryWrapper);
        }
    }
}
