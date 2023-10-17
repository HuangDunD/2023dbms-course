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

class IndexScanExecutor : public AbstractExecutor {
   private:
    std::string tab_name_;                      // 表名称
    TabMeta tab_;                               // 表的元数据
    std::vector<Condition> conds_;              // 扫描条件
    RmFileHandle *fh_;                          // 表的数据文件句柄
    std::vector<ColMeta> cols_;                 // 需要读取的字段
    size_t len_;                                // 选取出来的一条记录的长度
    std::vector<Condition> fed_conds_;          // 扫描条件，和conds_字段相同
    std::vector<ColMeta> lhs_cols_meta;       // condition列的meta

    IxIndexHandle *ix_;                          // 索引的数据文件句柄
    std::vector<std::string> index_col_names_;  // index scan涉及到的索引包含的字段
    IndexMeta index_meta_;                      // index scan涉及到的索引元数据

    std::vector<Rid> rids_;
    size_t rids_offset;
    Rid rid_;
    // std::unique_ptr<RecScan> scan_;
    std::unique_ptr<IxScan> ix_scan_;
    SmManager *sm_manager_;

   public:
    IndexScanExecutor(SmManager *sm_manager, std::string tab_name, std::vector<Condition> conds, std::vector<std::string> index_col_names,
                    Context *context) {
        sm_manager_ = sm_manager;
        context_ = context;
        tab_name_ = std::move(tab_name);
        tab_ = sm_manager_->db_.get_table(tab_name_);
        conds_ = std::move(conds);
        // index_no_ = index_no;
        index_col_names_ = index_col_names; 
        index_meta_ = *(tab_.get_index_meta(index_col_names_));
        fh_ = sm_manager_->fhs_.at(tab_name_).get();
        ix_ = sm_manager_->ihs_.at(sm_manager->get_ix_manager()->get_index_name(tab_name_, index_col_names_)).get();
        cols_ = tab_.cols;
        len_ = cols_.back().offset + cols_.back().len;
        std::map<CompOp, CompOp> swap_op = {
            {OP_EQ, OP_EQ}, {OP_NE, OP_NE}, {OP_LT, OP_GT}, {OP_GT, OP_LT}, {OP_LE, OP_GE}, {OP_GE, OP_LE},
        };

        for (auto &cond : conds_) {
            if (cond.lhs_col.tab_name != tab_name_) {
                // lhs is on other table, now rhs must be on this table
                assert(!cond.is_rhs_val && cond.rhs_col.tab_name == tab_name_);
                // swap lhs and rhs
                std::swap(cond.lhs_col, cond.rhs_col);
                cond.op = swap_op.at(cond.op);
            }
        }
        fed_conds_ = conds_;
        for(auto &cond: conds_){
            lhs_cols_meta.push_back(*tab_.get_col(cond.lhs_col.col_name));
        }
    }

    template<typename T, typename U>
    bool compare(const T& a, const U& b, CompOp op) {
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

    // 从seq_scan中拷贝
    bool check_cond(){
        std::unique_ptr<RmRecord> Tuple = Next();
        // TabMeta &lhs_tab = sm_manager_->db_.get_table(tab_name_);
        // for(auto &cond: conds_){
        for(size_t i=0; i<conds_.size(); i++){
            auto& cond = conds_[i];
            auto& lhs_col = lhs_cols_meta[i];
            // auto lhs_col = lhs_tab.get_col(cond.lhs_col.col_name);
            if(cond.is_rhs_val){
                char *lhs_data = Tuple->data + lhs_col.offset;
                bool res;
                if(lhs_col.type != cond.rhs_val.type){
                    if (lhs_col.type == TYPE_INT && cond.rhs_val.type == TYPE_BIGINT){
                        res = compare(*(long long *)lhs_data, cond.rhs_val.bigint_val, cond.op);
                    } else {
                        double lvalue, rvalue;
                        if (lhs_col.type == TYPE_BIGINT) {
                            lvalue = (double)*(long long *)lhs_data;
                        } else if (lhs_col.type == TYPE_FLOAT) {
                            lvalue = (double)*(float *)lhs_data;
                        }
                        
                        if (cond.rhs_val.type == TYPE_BIGINT) {
                            rvalue = (double)cond.rhs_val.bigint_val;
                        } else if (cond.rhs_val.type == TYPE_FLOAT) {
                            rvalue = (double)cond.rhs_val.float_val;
                        }
                        res = compare(lvalue, rvalue, cond.op);
                    }
                } else {
                    switch(lhs_col.type){
                        case TYPE_INT:
                            res = compare(*(int*)lhs_data, cond.rhs_val.int_val, cond.op);
                            break;
                        case TYPE_BIGINT:
                            res = compare(*(long long*)lhs_data, cond.rhs_val.bigint_val, cond.op);
                            break;
                        case TYPE_FLOAT:
                            res = compare(*(float*)lhs_data, cond.rhs_val.float_val, cond.op);
                            break;
                        case TYPE_STRING:
                        case TYPE_DATETIME:
                            res = compare(std::string(Tuple->data + lhs_col.offset, lhs_col.len), cond.rhs_val.getString(), cond.op);
                            break;                            
                    }
                }

                if (!res)
                    return false;
            } 
            else assert(0);
        }

        return true;
    }

    void setMaxKey(char* key, int off, int len, ColType type) {
        int int_val = INT32_MAX;
        float float_value = std::numeric_limits<float>::max();
        switch (type)
        {
        case ColType::TYPE_INT:
            memcpy(key + off, reinterpret_cast<char*>(&int_val),len); 
            break;
        case ColType::TYPE_FLOAT:
            memcpy(key + off, reinterpret_cast<char*>(&float_value),len);
            break;
        case ColType::TYPE_STRING:
            std::fill(key + off, key + off + len, 0xff);
            break;
        default:
            std::cout << "no match type in index" << std::endl;
            break;
        }
    }
    
    void setMinKey(char* key, int off, int len, ColType type) {
        int int_val = INT32_MIN;
        float float_value = std::numeric_limits<float>::min();
        switch (type)
        {
        case ColType::TYPE_INT:
            memcpy(key + off, reinterpret_cast<char*>(&int_val),len); 
            break;
        case ColType::TYPE_FLOAT:
            memcpy(key + off, reinterpret_cast<char*>(&float_value),len);
            break;
        case ColType::TYPE_STRING:
            std::fill(key + off, key + off + len, 0x00);
            break;
        default: 
            std::cout << "no match type in index" << std::endl;
            break;
        }
    }
    void beginTuple() override {
        // 索引扫描，给表上意向读锁
        // context_->lock_mgr_->lock_IS_on_table(context_->txn_, fh_->GetFd());
        context_->lock_mgr_->lock_shared_on_table(context_->txn_, fh_->GetFd());
        
        IxIndexHandle* ih = sm_manager_->ihs_.at(sm_manager_->get_ix_manager()->get_index_name(tab_name_, index_col_names_)).get();
        // 这里设置ix_scan的lowwer和uppper，注意: 当前的设置只适用于planner.cpp中的索引匹配规则get_index_cols()
        // assert(conds_.size() == index_col_names_.size());
        char* min_key = new char[index_meta_.col_tot_len];
        char* max_key = new char[index_meta_.col_tot_len];
        // 构造等值索引的key，注意，这里conds的顺序可能和索引顺序是不一致的
        int offset = 0;
        size_t cond_j = 0;
        for(size_t i = 0; i < (size_t)index_meta_.col_num; ++i) {
            bool max_set = false;
            bool min_set = false;
            bool equ_set = false;
            for( ; cond_j < conds_.size() && conds_[cond_j].lhs_col.col_name == index_meta_.cols[i].name; cond_j++){
                // assert(cond.op == CompOp::OP_EQ);
                // assert(cond.is_rhs_val);
                // if( cond.lhs_col.col_name == index_meta_.cols[i].name ){
                //     memcpy(key + offset, cond.rhs_val.raw.data, index_meta_.cols[i].len);
                //     offset += index_meta_.cols[i].len;
                //     break;
                // }
                // 这里cond的顺序已经处理好了
                if(conds_[cond_j].op == CompOp::OP_EQ){
                    memcpy(min_key + offset, conds_[cond_j].rhs_val.raw.data, index_meta_.cols[i].len);
                    memcpy(max_key + offset, conds_[cond_j].rhs_val.raw.data, index_meta_.cols[i].len);
                    equ_set = true;
                    // 跳过该列的其他condition
                    for(; cond_j < conds_.size() && conds_[cond_j].lhs_col.col_name == index_meta_.cols[i].name; cond_j++);
                    cond_j--;
                }
                else if(conds_[cond_j].op == CompOp::OP_LT || conds_[cond_j].op == CompOp::OP_LE){
                    // 小于、小于等于, 这里可能存在bug
                    if(!max_set){
                        memcpy(max_key + offset, conds_[cond_j].rhs_val.raw.data, index_meta_.cols[i].len);
                        max_set = true;
                    }
                    else{
                        //多个<, 取最小
                        std::vector<ColType> compare_col_type(1, index_meta_.cols[i].type);
                        std::vector<int> compare_col_len(1, index_meta_.cols[i].len);
                        if(ix_compare(conds_[cond_j].rhs_val.raw.data, max_key+offset, compare_col_type, compare_col_len) < 0) {
                            memcpy(max_key + offset, conds_[cond_j].rhs_val.raw.data, index_meta_.cols[i].len);
                        }
                    }
                }
                else if(conds_[cond_j].op == CompOp::OP_GT || conds_[cond_j].op == CompOp::OP_GE){
                    // 大于、大于等于, 这里可能存在bug
                    if(!min_set){
                        memcpy(min_key + offset, conds_[cond_j].rhs_val.raw.data, index_meta_.cols[i].len);
                        min_set = true;
                    }
                    else{
                        //多个>, 取最大
                        std::vector<ColType> compare_col_type(1, index_meta_.cols[i].type);
                        std::vector<int> compare_col_len(1, index_meta_.cols[i].len);
                        if(ix_compare(conds_[cond_j].rhs_val.raw.data, max_key+offset, compare_col_type, compare_col_len) > 0) {
                            memcpy(min_key + offset, conds_[cond_j].rhs_val.raw.data, index_meta_.cols[i].len);
                        }
                    }
                }
            }
            if(equ_set){
                offset += index_meta_.cols[i].len;
            }
            else if(!max_set && !min_set){
                setMaxKey(max_key, offset, index_meta_.cols[i].len, index_meta_.cols[i].type);
                setMinKey(min_key, offset, index_meta_.cols[i].len, index_meta_.cols[i].type);
                offset += index_meta_.cols[i++].len;
                while (i<(size_t)index_meta_.col_num){
                    setMaxKey(max_key, offset, index_meta_.cols[i].len, index_meta_.cols[i].type);
                    setMinKey(min_key, offset, index_meta_.cols[i].len, index_meta_.cols[i].type);
                    offset += index_meta_.cols[i++].len;
                }
                break;
            }
            else if(!max_set){
                setMaxKey(max_key, offset, index_meta_.cols[i].len, index_meta_.cols[i].type);
                offset += index_meta_.cols[i++].len;
                while (i<(size_t)index_meta_.col_num){
                    setMaxKey(max_key, offset, index_meta_.cols[i].len, index_meta_.cols[i].type);
                    setMinKey(min_key, offset, index_meta_.cols[i].len, index_meta_.cols[i].type);
                    offset += index_meta_.cols[i++].len;
                }
                break;
            }
            else if(!min_set){
                setMinKey(min_key, offset, index_meta_.cols[i].len, index_meta_.cols[i].type);
                offset += index_meta_.cols[i++].len;
                while (i<(size_t)index_meta_.col_num){
                    setMaxKey(max_key, offset, index_meta_.cols[i].len, index_meta_.cols[i].type);
                    setMinKey(min_key, offset, index_meta_.cols[i].len, index_meta_.cols[i].type);
                    offset += index_meta_.cols[i++].len;
                }
                break;
            }
            else{
                offset += index_meta_.cols[i].len;
            }
        }
        // Iid lower_bound, upper_bound;
        
        Iid lower_bound = ih->lower_bound(min_key);
        Iid upper_bound = ih->upper_bound(max_key);

        // 这里对表一次性上间隙锁
        // ih->gap_lock(min_key, max_key, rids_, context_, fh_->GetFd());

        for(ix_scan_ = std::make_unique<IxScan>(ih,lower_bound,upper_bound,sm_manager_->get_bpm()); !is_end(); ix_scan_->next()){
            // context_->lock_mgr_->lock_gap_on_index(context_->txn_, ix_scan_->rid(), ix_->GetFd());
            // context_->lock_mgr_->lock_shared_on_record(context_->txn_, ix_scan_->rid(), fh_->GetFd());
            if(!check_cond()) continue;
            else break;
        };

        for(rids_offset =0; rids_offset < rids_.size(); rids_offset++){
            rid_ = rids_[rids_offset];
            if(!check_cond()) continue;
            else break;
        }

        delete[] min_key;
        delete[] max_key;
        
        return;
    }

    void nextTuple() override {
        for(ix_scan_->next(); !is_end(); ix_scan_->next()){
            // context_->lock_mgr_->lock_shared_on_record(context_->txn_, ix_scan_->rid(), fh_->GetFd());
            if(!check_cond()) continue;
            else break;
        }
        
        // for( ++rids_offset; rids_offset < rids_.size(); rids_offset++){
        //     rid_ = rids_[rids_offset];
        //     if(!check_cond()) continue;
        //     else break;
        // }
        return;
    }

    bool is_end() const override { 
        return ix_scan_->is_end();
        // return rids_offset >= rids_.size();
    }

    std::unique_ptr<RmRecord> Next() override {
        rid_ = ix_scan_->rid();
        return fh_->get_record(rid_, nullptr);
    }

    Rid &rid() override { return rid_; }
    
    const std::vector<ColMeta> &cols() const override { return cols_;};

    size_t tupleLen() override { return len_;}

};