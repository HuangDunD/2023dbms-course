/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#pragma once
#include "execution_defs.h"
#include "execution_manager.h"
#include "executor_abstract.h"
#include "index/ix.h"
#include "system/sm.h"

#include <map>

class JoinBlock
{
public:
    size_t block_size;   // 目前block_size = PGAE_SIZE, 后面可以调节
    size_t records_per_block;
    size_t record_len;
    size_t tup_total;
    size_t tup_in_block;
    char* data;
    char* curr_pos; // 方便下次插入数据, 感觉比idx好用
    FILE* fp;
    std::string f_path;

    // use for abort tansaction because of open file failure.
    Context* context_;
public:
    JoinBlock(size_t record_len, std::string f_path, Context* context): 
                                  block_size(PAGE_SIZE), 
                                  records_per_block(block_size/record_len),
                                  record_len(record_len),
                                  tup_total(0),
                                  tup_in_block(0),
                                  f_path(f_path),
                                  context_(context)
    {
        data = (char*)malloc(block_size);
        if(data == nullptr)  // 真是一点内存都没有了
            assert(0);
        
        curr_pos = data;

        // 打开文件, 这种还可以清空文件之前的内容
        fp = fopen(f_path.c_str(),"w+");
        if(fp == nullptr){
            context_->txn_->set_state(TransactionState::ABORTED);
            throw TransactionAbortException(context_->txn_->get_transaction_id(), AbortReason::NESTLOOPJOIN_FILE_FAILURE);
        }
            // assert(0);
    }
    
    inline int insert_tup(char* tup_data)
    {
        memcpy(curr_pos,tup_data,record_len);

        curr_pos += record_len;
        tup_total++;
        tup_in_block++;

        if(curr_pos+record_len > data+block_size) // 判断下次写入会不会超过block
        {
            // 写入磁盘, 指针,idx等归位
            size_t rv = fwrite(data, record_len, records_per_block, fp);
            if(rv != records_per_block)
                assert(0);
            curr_pos = data;
            tup_in_block = 0;
        }
        return 0;
    }

    void reset_to_block_head()
    {
        curr_pos = data;
    }

    void reset_to_file_head()
    {
        rewind(fp); // 把读头复位到文件开头
        tup_in_block = fread(data, record_len, records_per_block, fp);  // 再存进去
        curr_pos = data;
    }

    void start_read()
    {
        fwrite(data, record_len, tup_in_block, fp); // 把剩下的全部写入磁盘
        reset_to_file_head();
    }

    inline char* read_tup()
    {
        if(curr_pos >= data + tup_in_block * record_len)
            return nullptr;
        
        char* curr = curr_pos;
        curr_pos += record_len;
        return curr;
    }

    inline int read_next_block()
    {
        tup_in_block = fread(data, record_len, records_per_block, fp);
        curr_pos = data;
        return tup_in_block;
    }

    ~JoinBlock()
    {
        if(data != nullptr)
            free(data);
        fclose(fp);
        remove(f_path.c_str());
    }

};
class NestedLoopJoinExecutor : public AbstractExecutor {
   private:
    std::unique_ptr<AbstractExecutor> left_;    // 左儿子节点（需要join的表）
    std::unique_ptr<AbstractExecutor> right_;   // 右儿子节点（需要join的表）
    size_t len_;                                // join后获得的每条记录的长度
    std::vector<ColMeta> cols_;                 // join后获得的记录的字段

    std::vector<Condition> fed_conds_;          // join条件
    bool isend;

    std::map<TabCol,ColMeta> col_offset_map; // 直接先存起来, 方便condition判断

    char* left_tup;
    char* right_tup;
    // std::unique_ptr<RmRecord> matched_tup;
    char* matched_data;
    JoinBlock left_block; 
    JoinBlock right_block;

   public:
    NestedLoopJoinExecutor(std::unique_ptr<AbstractExecutor> left, 
                           std::unique_ptr<AbstractExecutor> right, 
                           std::vector<Condition> conds,
                           Context *context) :
                                left_(std::move(left)),
                                right_(std::move(right)),
                                // left_block(left_->tupleLen(), (left_->cols()[0].tab_name+"-left.tmp")),
                                // right_block(right_->tupleLen(), (right_->cols()[0].tab_name+"-right.tmp"))
                                // // 这里还需要添加一下id, 不然会冲突
                                left_block(left_->tupleLen(), (left_->cols()[0].tab_name+std::to_string(pthread_self())+"-left.tmp"), context),
                                right_block(right_->tupleLen(), (right_->cols()[0].tab_name+std::to_string(pthread_self())+"-right.tmp"), context)
                            {
        context_ = context;
        
        len_ = left_->tupleLen() + right_->tupleLen();
        cols_ = left_->cols();
        // context_->txn_->get_thread_id();
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
        isend = false;
        fed_conds_ = std::move(conds);
    }

    ~NestedLoopJoinExecutor() { delete[] matched_data; }

    void beginTuple() override {
        // 1. init
        // 2. 调用continue_block_join(), 尝试找满足条件的元组

        int rv = init_join();
        if(rv == -1)
            isend = true;
        else
            continue_block_join();
    }

    void nextTuple() override {
        continue_block_join();
    }

    std::unique_ptr<RmRecord> Next() override {
        // 返回匹配的元组
        // 用户需要先调用beginTuple, 然后nextTuple, 最后调用Next()
        return std::make_unique<RmRecord>(len_, matched_data);
    }

    // join 不需要rid
    Rid &rid() override { return _abstract_rid; }

    bool is_end() const override {
        return isend;
    }

    const std::vector<ColMeta> &cols() const override { return cols_;};

    size_t tupleLen() override { return len_; }
   private:
    int init_join()
    {
        // 初始化一些条件, 如果失败(比如一开始就有空表), 则返回-1 
        //      子节点的两个表导入block中
        //      isend
        //      fed_cond_: 过滤出需要的 condition

        // 好像没有必要处理空表的情况

        for(left_->beginTuple();!left_->is_end();left_->nextTuple())
            left_block.insert_tup(left_->Next()->data);
        left_block.start_read();
        left_tup = left_block.read_tup();  // left_tup 必须要先读, 因为continue_block_join一开始默认left_tup有数据

        for(right_->beginTuple();!right_->is_end();right_->nextTuple())
            right_block.insert_tup( right_->Next()->data);
        right_block.start_read();

        isend=false;  // isend一开始必须为false, 后面发现异常时才被设置为true, 不然do-while过不了

        std::vector<Condition> filtered_conds;
        for(auto& cond:fed_conds_)
        {
            if(!cond.is_rhs_val)
            {
                // 都能找到, 说明属于本次join的condition
                if(col_offset_map.find(cond.lhs_col) != col_offset_map.end()
                    && col_offset_map.find(cond.rhs_col) != col_offset_map.end())
                {
                    filtered_conds.push_back(cond);
                }
            }
        }
        fed_conds_ = filtered_conds;
        return 0;
    }


    inline void continue_block_join()
    {
        // 该函数找到一条匹配的元组后就返回
        bool ismatched = false;

        while(left_tup!=nullptr)
        {
            right_tup = right_block.read_tup();
            while(right_tup != nullptr)
            {
                ismatched = check_cond();
                if(ismatched)
                {
                    // 把结果拼起来
                    isend = false;
                    int left_size = left_->tupleLen();
                    int right_size = right_->tupleLen();

                    memcpy(matched_data, left_tup, left_size);
                    memcpy(matched_data+left_size, right_tup, right_size);
                    return;
                }
                right_tup = right_block.read_tup();
            }
            left_tup = left_block.read_tup();
            right_block.reset_to_block_head();
        }

        // 如果能执行到这里, 说明两个块没有一个匹配
        // 更新block:
        //      尝试读取右表下一个block, 然后将左表归位
        // 然后递归执行
        int tup_cnt = right_block.read_next_block();
        if(tup_cnt == 0) // 不能读出右表块, 说明右表到头了, 该读下一个左表的块, 右表读头归位到最开始的位置
        {
            tup_cnt = left_block.read_next_block();
            if(tup_cnt == 0)  // 说明左表到头了, 连接就结束了
            {
                isend = true;
                return;
            }
            else // 没有到头, 继续从新的左表块中读
                left_tup = left_block.read_tup();

            right_block.reset_to_file_head(); // 右表读头归位
        }
        else // 能够读出右表块, 说明没到头, 左表块读头归位
        {
            left_block.reset_to_block_head();
            left_tup = left_block.read_tup();
        }

        continue_block_join(); // 递归执行
    }

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

    bool check_cond()
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
};
