#pragma once
#include "execution_defs.h"
#include "execution_manager.h"
#include "executor_abstract.h"
#include "index/ix.h"
#include "system/sm.h"

#define PARTITION_NUM 256
#define PARTITION_BIT 8 // 这个跟上面是对应的

//专门负责partition
class Partition
{
private:
    int tup_total_count[PARTITION_NUM]; // 全局count
    int tup_count[PARTITION_NUM]; // 部分count
    char* part_pages;
    int mask;
    int page_size; // 目前partition page = 1个page_size, 方便后续替换
    int record_len;
    ColMeta meta;
    char* last_malloc_part; // 记录上一个分配空间的partition, 在分配下一个partition的时候进行释放

    // 根据tup和meta, 计算hash值
    inline int get_hash_code(char* tup, ColMeta& meta)
    {   
        int code = 0;
        switch (meta.type)
        {
        case TYPE_INT:
            code = *(int*)(tup+meta.offset);
            code = code & mask;
            break;
        
        default:
            assert(0); // hash join目前只支持int类型, 其他的均会报错
            break;
        }
        return code;
    }

    inline void flush_to_file(int hash_code, char* part_base)
    {
        std::string file_path = "./part";
        file_path += std::to_string(pthread_self())+"-";
        file_path += hash_code;
        file_path += ".txt";

        // 打开文件, 不存在则创建, 追加模式, 读头默认在尾部
        FILE* fp = fopen(file_path.c_str(),"a");
        fwrite(part_base,record_len,tup_count[hash_code],fp);
        tup_count[hash_code] = 0;
        fclose(fp);
    }

public:

    Partition(int record_len, ColMeta meta): page_size(PAGE_SIZE), 
                                             record_len(record_len),
                                             meta(meta),
                                             last_malloc_part(nullptr) {
        part_pages = (char*)calloc(PARTITION_NUM,page_size);
        if(part_pages == nullptr)
            assert(0);
        
        memset(tup_total_count,0,sizeof(int)*PARTITION_NUM);
        memset(tup_count,0,sizeof(int)*PARTITION_NUM);
        mask = (1 << PARTITION_BIT) - 1;
    }

    ~Partition()
    {
        // 删除之前创建的文件
        int records_per_page = page_size / record_len;
        for(int i=0;i<PARTITION_NUM;i++)
        {
            if(tup_total_count[i] > records_per_page)
            {
                // 删除
                std::string file_path = "./part";
                file_path += std::to_string(pthread_self())+"-";
                file_path += i;
                file_path += ".txt";
                remove(file_path.c_str());
            }
        }

        // 清空占用的空间
        free(part_pages);
    }

    inline void put_tup(char* tup)
    {
        int code = get_hash_code(tup,meta);
        tup_total_count[code]++;
        int occupieded = record_len * tup_count[code];
        char* part_base = part_pages + code * page_size; // 该partition的起始地址
        
        if(occupieded + record_len > page_size) // 说明满了
        {
            // 将该partition的内容flush到临时文件中
            flush_to_file(code, part_base);
            occupieded = 0;
        }

        char* curr_pos = part_base + occupieded;
        memcpy(curr_pos,tup,record_len);
        tup_count[code]++;
    }

    int get_partition(int idx, char** part, int* tup_count)
    {
        // 传入partition的idx, 返回指向partition的指针, 结果存在part变量中
        // 如果该partition超过page的大小, 则申请能够完全容纳partition大小的空间
        // 返回值为0, 说明指向的空间不需要释放, 返回值为1说明需要释放, 但是现在不需要用了
        if(idx >= PARTITION_NUM)
        {
            *part = nullptr;
            *tup_count = 0;
            assert(0); // 还是弄个安全检查吧
        }

        *tup_count = tup_total_count[idx];
        int records_per_page = page_size / record_len;
        if(tup_total_count[idx] > records_per_page)
        {
            if(last_malloc_part != nullptr)
            {
                free(last_malloc_part);
                last_malloc_part = nullptr;
            }
            char* part_base = (char*)malloc(record_len * tup_total_count[idx]);
            if(part_base == nullptr)
                assert(0); 
            
            last_malloc_part = part_base;

            std::string file_path = "./part";
            file_path += idx;
            file_path += ".txt";
            FILE* fp = fopen(file_path.c_str(),"r");
            int count = fread(part_base,record_len,tup_total_count[idx],fp);
            if(count != tup_total_count[idx])
                assert(0);

            memcpy(part_base + record_len * count,
                    part_pages + idx * page_size,
                    record_len*tup_count[idx]);
            *part = part_base;
            return 1;
        }
        else
        {
            char* part_base = part_pages + idx * page_size; 
            *part = part_base;
            return 0;
        }
    }

    ColMeta get_partition_meta() {return meta;}
};

typedef struct HashJoinContext
{
    int* hash_table;
    int* next;
    int i;
    int target; // 存的是下次应该check的target
    char* curr_tup;

    // 连接匹配的结果
    char* matched_tup_left;
    char* matched_tup_right;
}HashJoinContext;


// 针对每一对partition执行Hash join
class PartitionHashJoin
{
private:
    std::vector<Condition> fed_conds_;          // join条件
    
    char* left_rel;
    int left_num;
    int left_len;  
    ColMeta left_join_col;      // 由于左右两个col可能不同, 因此join col需要单独存

    char*right_rel;
    int right_num;
    int right_len;
    ColMeta right_join_col;

    std::map<TabCol,ColMeta> col_offset_map; // 直接先存起来, 方便condition判断

    int mask; // 目前只支持int类型, mask直接放这里, 省去反复构造
    HashJoinContext context; // join所需的上下文

    template<typename T>
    inline bool compare(const T& a, const T& b, CompOp op)
    {
        switch (op) {
            case OP_EQ:
                return a == b;
            case OP_NE:
                return a != b;
            case OP_LT:
                return a < b;
            case OP_GT:
                return a > b;
            case OP_LE:
                return a <= b;
            case OP_GE:
                return a >= b;
            default:
                std::cout << "Invalid comparison operator" << std::endl;
                assert(0);
                return false;
        }
    }

    bool check_cond(char* left_tup, char* right_tup)
    {
        bool res=true; // 默认为true, 因为如果fed_conds_为空, 则直接返回的res应该为true

        // 当前假设连接就只有一个连接条件, 判断结果以最后的那个fed_conds_为标准
        for(auto& cond:fed_conds_)
        {
            ColMeta left_meta = col_offset_map.find(cond.lhs_col)->second;
            ColMeta right_meta = col_offset_map.find(cond.rhs_col)->second;

            char* left_val = left_tup + left_meta.offset;
            char* right_val = right_tup + right_meta.offset;

            if(left_meta.type == right_meta.type)
            {
                switch (left_meta.type)
                {
                    case TYPE_INT: // "与" 运算, 必须要同时满足
                        res = res && compare(*(int*)left_val, *(int*)right_val, cond.op);
                        break;
                    case TYPE_BIGINT:
                        res = res && compare(*(long long*)left_val, *(long long*)right_val, cond.op);
                        break;
                    case TYPE_FLOAT:
                        res = res && compare(*(float*)left_val, *(float*)right_val, cond.op);
                        break;
                    case TYPE_STRING:
                    {
                        std::string l_string = std::string(left_val, left_meta.len);
                        std::string r_string = std::string(right_val, right_meta.len);
                        res = res && compare(l_string, r_string, cond.op);
                        break;
                    }
                    default:
                        printf("error, invalid data type at %s\n", __func__);
                        exit(-1);
                        break;
                }
            }
            else
            {
                // 别吧, 我还得再类型转换
                assert(0); // 类型不同, 直接不比较, 这样很合理吧
                return false;
            }
        }
        
        return res;
    }

    inline int get_hash_code(char* tup, ColMeta& meta)
    {   
        int code = 0;
        switch (meta.type)
        {
        case TYPE_INT:
            code = *(int*)(tup+meta.offset);
            code = (code & mask) >> PARTITION_BIT;
            break;
        default:
            assert(0); // hash join目前只支持int类型, 其他的均会报错
            break;
        }
        return code;
    }
    
    int upround_power2(int n)
    {
        // 返回大于或等于n, 并且是最小的二进制幂次数
        // 比如:
        // 输入3, 返回4
        // 输入5, 返回8
        // 输入8, 返回8
        // 输入13, 返回16
        if(n == 0)
            return 1;
        n--;
        n |= n >> 1; 
        n |= n >> 2; 
        n |= n >> 4; 
        n |= n >> 8; 
        n |= n >> 16;
        n++;
        return n;
    }

public:
    PartitionHashJoin(char* left_rel, int left_num, int left_len,ColMeta left_join_col,
                     char*right_rel, int right_num, int right_len,ColMeta right_join_col,
                     std::map<TabCol,ColMeta>& col_offset_map,
                     std::vector<Condition>& fed_conds_):
                        fed_conds_(fed_conds_),
                        left_rel(left_rel), left_num(left_num), left_len(left_len),left_join_col(left_join_col),
                        right_rel(right_rel), right_num(right_num), right_len(right_len),right_join_col(right_join_col),
                        col_offset_map(col_offset_map) {
        context.hash_table = nullptr;
        context.next = nullptr;
        context.i = 0;
        context.target = 0;
        context.matched_tup_left = nullptr;
        context.matched_tup_right = nullptr;
        context.curr_tup = right_rel;
    }

    void build_hash_table()
    {
        // 暂时就用左侧来建hash table
        int bucket_num = upround_power2(left_num);
        int limit = 2;  // 缩小 2^limit倍, 相当于每个bucket可容纳 2^limit 个tuple左右
        bucket_num = bucket_num >> limit;
        if(bucket_num < (2^limit))
            bucket_num = 2^limit;
        int* hash_table = (int*)calloc(bucket_num,sizeof(int));
        int* next = (int*)malloc(left_num*sizeof(int));
        if(hash_table == nullptr || next == nullptr)
            assert(0);
        
        mask = ((1 << limit) - 1) << PARTITION_BIT;
        int code;
        char* curr_tup = left_rel;
        for(int i=0;i<left_num;i++)
        {
            code = get_hash_code(curr_tup,left_join_col);
            next[i] = hash_table[code];
            hash_table[code] = i+1; // 下标都是从1开始, 如果为0说明为空
            curr_tup += left_len;
        }

        context.hash_table = hash_table;
        context.next = next;

        // 在build的同时,为第一个probe做准备
        code = get_hash_code(curr_tup,right_join_col);
        context.target = hash_table[code];
    }

    int continue_probe()
    {
        // 对该partition继续执行probe操作, 直至找到匹配
        // 若最终找到匹配, 返回1, 否则返回0
        char *left_tup;
        int target = context.target, code;
        int* hash_table = context.hash_table;
        int* next = context.next;

        char* curr_tup = context.curr_tup;
        // 1. 首先将上次没弄完的target弄完
        while(target)
        {
            left_tup = left_rel + left_len * (target - 1);
            bool res = check_cond(left_tup,curr_tup);
            target = next[target-1];
            if(res) // 说明match
            {
                context.matched_tup_left = left_tup;
                context.matched_tup_right = curr_tup;
                context.target = target;
                return 1;
            }
        }

        // 2. 然后从下一个开始继续找
        curr_tup += right_len;
        for(int i=context.i;i<right_num;i++)
        {
            code = get_hash_code(curr_tup,right_join_col);
            target = hash_table[code];
            // index 都是从1开始, 在索引时需要先减去1
            while(target)
            {
                left_tup = left_rel + left_len * (target - 1);
                bool res = check_cond(left_tup,curr_tup);
                target = next[target-1];
                if(res) // 说明match
                {
                    context.matched_tup_left = left_tup;
                    context.matched_tup_right = curr_tup;
                    context.target = target;
                    context.i = i+1;
                    context.curr_tup = curr_tup;
                    return 1;
                }
            }
            curr_tup += right_len;
        }

        return 0; // 说明没有匹配了
    }

    void get_matched_tup(char** left_tup, char** right_tup)
    {
        *left_tup = context.matched_tup_left;
        *right_tup = context.matched_tup_right;
    }
    // 更新下一个partition
    void update_next_partition(char* new_left_rel, int new_left_num, 
                               char* new_right_rel, int new_right_num) {
        left_rel = new_left_rel;
        left_num = new_left_num;

        right_rel = new_right_rel;
        right_num = new_right_num;

        // context更新
        free(context.hash_table);
        free(context.next);

        context.i = 0;
        context.target = 0;
        context.matched_tup_left = nullptr;
        context.matched_tup_right = nullptr;
        context.hash_table = nullptr;
        context.next = nullptr;
        context.curr_tup = right_rel;
    }
};

class HashJoinExecutor : public AbstractExecutor {
    // 先将输入的tuple划分至 2^8=256 个partition, 然后针对每个partition执行hash join
    // 每个partition在连接时必须有足够的内存, 能放得下要连接的partition
    // 否则就报错(简单粗暴, 不然还得考虑磁盘换入换出, 太麻烦了)

private:
    std::unique_ptr<AbstractExecutor> left_;    // 左儿子节点（需要join的表）
    std::unique_ptr<AbstractExecutor> right_;   // 右儿子节点（需要join的表）
    size_t len_;                                // join后获得的每条记录的长度
    std::vector<ColMeta> cols_;                 // join后获得的记录的字段

    std::vector<Condition> fed_conds_;          // join条件
    bool isend;

    std::map<TabCol,ColMeta> col_offset_map; // 直接先存起来, 方便condition判断

    Partition* left_part;
    Partition* right_part;

    char* matched_data;

    PartitionHashJoin* join;
    int partition_idx;  // 表示下一次应该进行连接的partition的 idx

private:
    int continue_join()
    { // 执行连接, 直至找到匹配, 将结果保存在matched_data中
      // 若存在匹配则返回1, 若不存在匹配, 返回0
        int rv = join->continue_probe();
        if(rv == 0) // 说明该partition找不到连接结果了, 切换下一对partition
        {
            char* left_rel,*right_rel;
            int left_num=0,right_num=0;
            for(int i=partition_idx;i<PARTITION_NUM;i++)
            {
                left_part->get_partition(i,&left_rel,&left_num);
                right_part->get_partition(i,&right_rel,&right_num);
                if(left_num != 0 && right_num !=0)
                {
                    partition_idx = i+1;
                    join->update_next_partition(left_rel,left_num,right_rel,right_num);
                    join->build_hash_table();
                    break;
                }
            }
            if(left_num == 0 || right_num == 0)
                return 0;
            return this->continue_join(); // 切换到新的partition后, 递归调用执行连接
        }
        else{ // 说明找到了, 将结果取出
            char* left_tup, *right_tup;
            join->get_matched_tup(&left_tup,&right_tup);
            memcpy(matched_data,left_tup,left_->tupleLen());
            memcpy(matched_data+left_->tupleLen(),right_tup,right_->tupleLen());
            return 1;
        }
    }

public:
    HashJoinExecutor(std::unique_ptr<AbstractExecutor> left, 
                    std::unique_ptr<AbstractExecutor> right, 
                    std::vector<Condition> conds):
                    left_(std::move(left)),
                    right_(std::move(right)),
                    isend(false),
                    partition_idx(0) {
        len_ = left_->tupleLen() + right_->tupleLen();
        cols_ = left_->cols();
        matched_data = new char[len_];

        std::vector<ColMeta> left_cols = cols_;
        for(ColMeta col:left_cols)
        {
            TabCol temp;
            temp.col_name = col.name;
            temp.tab_name = col.tab_name;
            col_offset_map.insert({temp,col});
        }

        std::vector<ColMeta> right_cols = right_->cols();
        for(ColMeta col:right_cols)
        {
            TabCol temp;
            temp.col_name = col.name;
            temp.tab_name = col.tab_name;
            col_offset_map.insert({temp,col});
        }

        // 每个元组属性的offset在left属性后面
        for (auto &col : right_cols) {
            col.offset += left_->tupleLen();
        }

        cols_.insert(cols_.end(), right_cols.begin(), right_cols.end());

        for(auto& cond:conds)
        { // 右值是列属性, 并且都能找到, 说明属于本次join的condition
            if(!cond.is_rhs_val &&
                col_offset_map.find(cond.lhs_col) != col_offset_map.end() &&
                col_offset_map.find(cond.rhs_col) != col_offset_map.end())
            fed_conds_.push_back(cond);
        }

        // 找到join key的col属性
        for(auto i:fed_conds_)
        {
            // 以第一个等值连接条件作为join col
            if(i.op == OP_EQ)
            {
                ColMeta left_col_meta = col_offset_map.find(i.lhs_col)->second;
                ColMeta right_col_meta = col_offset_map.find(i.rhs_col)->second;
                left_part = new Partition(left_->tupleLen(),left_col_meta);
                right_part = new Partition(right_->tupleLen(),right_col_meta);
                break;
            }
        }
    }

    void beginTuple() override {
        // 找到第一个matched tuple
        // 这里会做一些初始化的操作

        // 1. 将左右两个表进行partition
        for(left_->beginTuple();!left_->is_end();left_->nextTuple())
            left_part->put_tup(left_->Next()->data);
        
        for(right_->beginTuple();!right_->is_end();right_->nextTuple())
            right_part->put_tup(right_->Next()->data);
        char* left_rel,*right_rel;
        int left_num,right_num;

        // 2. 选择一对非空的partition, 然后构造join实例进行连接
        for(int i=0;i<PARTITION_NUM;i++)
        {
            left_part->get_partition(i,&left_rel,&left_num);
            right_part->get_partition(i,&right_rel,&right_num);
            if(left_num != 0 && right_num !=0)
            {
                partition_idx = i+1;
                // partition的col同时作为建立hash table的col
                join = new PartitionHashJoin(left_rel,left_num,left_->tupleLen(),left_part->get_partition_meta(),
                                            right_rel,right_num,right_->tupleLen(),right_part->get_partition_meta(),
                                            col_offset_map,fed_conds_);
                break;
            }
        }
        join->build_hash_table();

        int rv = continue_join();
        if(rv == 0)
            isend = true;
    }

    void nextTuple() override {
        int rv = continue_join();
        if(rv == 0)
            isend = true;
    }

    std::unique_ptr<RmRecord> Next() override {
        // 返回匹配的元组
        // 用户需要先调用beginTuple, 然后nextTuple, 最后调用Next()
        return std::make_unique<RmRecord>(len_, matched_data);
    }

    size_t tupleLen() override { return len_; }
    const std::vector<ColMeta> &cols() const override { return cols_;};
    bool is_end() const override { return isend; }
    Rid &rid() override { return _abstract_rid; } // join 不需要rid

    ~HashJoinExecutor()
    {
        delete left_part;
        delete right_part;
        delete join;
        delete[] matched_data;
    }

};

