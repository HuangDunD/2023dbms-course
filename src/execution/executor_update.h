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
#include "common/tools.h"

class UpdateExecutor : public AbstractExecutor {
   private:
    TabMeta tab_;
    std::vector<Condition> conds_;
    RmFileHandle *fh_;
    std::vector<Rid> rids_;
    std::string tab_name_;
    std::vector<SetClause> set_clauses_;
    SmManager *sm_manager_;

   public:
    UpdateExecutor(SmManager *sm_manager, const std::string &tab_name, std::vector<SetClause> set_clauses,
                   std::vector<Condition> conds, std::vector<Rid> rids, Context *context) {
        sm_manager_ = sm_manager;
        tab_name_ = tab_name;
        set_clauses_ = set_clauses;
        tab_ = sm_manager_->db_.get_table(tab_name);
        fh_ = sm_manager_->fhs_.at(tab_name).get();
        conds_ = conds;
        rids_ = rids;
        context_ = context;
    }
    std::unique_ptr<RmRecord> Next() override {
        
        // if(tab_.indexes.size() == 0){
        //     // 没有索引，即没有间隙，直接上X锁
        //     context_->lock_mgr_->lock_exclusive_on_table(context_->txn_, fh_->GetFd());
        // }
        // else{
        //     // 对表上IX锁
        //     context_->lock_mgr_->lock_IX_on_table(context_->txn_, fh_->GetFd());
        // }

        TabMeta &lhs_tab = sm_manager_->db_.get_table(tab_name_);
        // 索引维护
        std::vector<std::vector<char*>> delete_index_key(tab_.indexes.size());
        std::vector<std::vector<char*>> insert_index_key(tab_.indexes.size());
        std::vector<std::unique_ptr<RmRecord>> new_tuple;

        for(auto &rid: rids_){
            // 对行上X锁
            // context_->lock_mgr_->lock_exclusive_on_record(context_->txn_, rid, fh_->GetFd());

            std::unique_ptr<RmRecord> Tuple = fh_->get_record(rid, context_);
            for(size_t i = 0; i < tab_.indexes.size(); ++i) {
                auto& index = tab_.indexes[i];
                auto ih = sm_manager_->ihs_.at(sm_manager_->get_ix_manager()->get_index_name(tab_name_, index.cols)).get();
                char* key = new char[index.col_tot_len];
                int offset = 0;
                for(size_t i = 0; i < (size_t)index.col_num; ++i) {
                    memcpy(key + offset, Tuple->data + index.cols[i].offset, index.cols[i].len);
                    offset += index.cols[i].len;
                }
                delete_index_key[i].push_back(key);
                // 删除索引
                ih->delete_entry(key, context_->txn_);
            }
            for(auto &set: set_clauses_){
                auto lhs_col = lhs_tab.get_col(set.lhs.col_name);
                set.rhs.convert_value_to_col_type_and_fill(*lhs_col);
                if (!set.rhs_col.col_name.empty()) {
                    auto rhs_col = lhs_tab.get_col(set.rhs_col.col_name);
                    Value tmp;
                    if(lhs_col->type == TYPE_INT){
                        int old_val = *(int *)(Tuple->data + rhs_col->offset);
                        tmp.set_int(old_val + set.rhs.int_val);
                    } else if(lhs_col->type == TYPE_BIGINT){
                        long long old_val = *(long long *)(Tuple->data + rhs_col->offset);
                        tmp.set_bigint(old_val + set.rhs.bigint_val);
                    } else if(lhs_col->type == TYPE_FLOAT){
                        float old_val = *(float *)(Tuple->data + rhs_col->offset);
                        tmp.set_float(old_val + set.rhs.float_val);
                    } else {
                        throw std::runtime_error("unsupported type");
                    }
                    memcpy(Tuple->data + lhs_col->offset, tmp.raw.data, lhs_col->len);
                } else {
                    memcpy(Tuple->data + lhs_col->offset, set.rhs.raw.data, lhs_col->len);
                }
            }
            for(size_t i = 0; i < tab_.indexes.size(); ++i) {
                auto& index = tab_.indexes[i];
                char* key = new char[index.col_tot_len];
                int offset = 0;
                for(size_t i = 0; i < (size_t)index.col_num; ++i) {
                    memcpy(key + offset, Tuple->data + index.cols[i].offset, index.cols[i].len);
                    offset += index.cols[i].len;
                }
                insert_index_key[i].push_back(key);
            }
            new_tuple.push_back(std::move(Tuple));
        }
        // 检查索引是否可能冲突
        bool is_index_conflict = false;
        for(size_t i = 0; i < tab_.indexes.size(); ++i) {
            auto& index = tab_.indexes[i];
            auto ih = sm_manager_->ihs_.at(sm_manager_->get_ix_manager()->get_index_name(tab_name_, index.cols)).get();
            assert(insert_index_key[0].size() == rids_.size());
            assert(delete_index_key[0].size() == rids_.size());
            for(size_t j = 0; j < rids_.size(); j++){
                
                try{
                    // 检查是否是可插入的
                    // Iid insert_upper_iid = ih->upper_bound(insert_index_key[i][j]);
                    // Rid gap_rid = ih->get_rid(insert_upper_iid);
                    // context_->lock_mgr_->try_lock_in_gap(context_->txn_, gap_rid, ih->GetFd());
                }
                catch (TransactionAbortException &e) {
                    // 恢复索引
                    for(size_t i = 0; i < tab_.indexes.size(); ++i) {
                        auto& index = tab_.indexes[i];
                        auto ih = sm_manager_->ihs_.at(sm_manager_->get_ix_manager()->get_index_name(tab_name_, index.cols)).get();
                        for(size_t j = 0; j < rids_.size(); j++){
                            ih->insert_entry(delete_index_key[i][j], rids_[j], context_->txn_);
                        }
                    }
                    throw TransactionAbortException (context_->txn_->get_transaction_id(), AbortReason::DEADLOCK_PREVENTION); 
                }
                
                // 首先检查这个vector中是否有重复的key
                for(size_t k = j + 1; k < rids_.size(); k++){
                    if(memcmp(insert_index_key[i][j], insert_index_key[i][k], index.col_tot_len) == 0) {
                        is_index_conflict = true;
                        break;
                    }
                }

                if(is_index_conflict) break;

                // 之后检查与剩余的索引是否冲突
                std::vector<Rid> tmp_val;
                if(ih->get_value(insert_index_key[i][j], &tmp_val, context_->txn_) == true){
                    is_index_conflict = true;
                    break;
                }
            }
            if(is_index_conflict) break;
        }
        if(!is_index_conflict){
            // 不冲突
            assert(rids_.size() == new_tuple.size());
            // 添加索引
            for(size_t i = 0; i < tab_.indexes.size(); ++i) {
                auto& index = tab_.indexes[i];
                auto ih = sm_manager_->ihs_.at(sm_manager_->get_ix_manager()->get_index_name(tab_name_, index.cols)).get();
                for(size_t j = 0; j < rids_.size(); j++){
                    ih->insert_entry(insert_index_key[i][j], rids_[j], context_->txn_);
                }
            }            
            for(size_t k = 0; k < rids_.size(); k++){
                // 将写入数据添加到写集
                auto Tuple = fh_->get_record(rids_[k], context_);
                if(context_->txn_ != nullptr){
                    WriteRecord* write_record = new WriteRecord(WType::UPDATE_TUPLE, tab_name_, rids_[k], *Tuple.get());
                    context_->txn_->append_write_record(write_record);
                }
                if(enable_logging){
                    //写Update
                    UpdateLogRecord record(context_->txn_->get_transaction_id(), context_->txn_->get_prev_lsn(), *Tuple.get(), *new_tuple[k].get(), rids_[k], tab_name_);
                    lsn_t lsn = context_->log_mgr_->add_log_to_buffer(&record);
                    context_->txn_->set_prev_lsn(lsn);
                    Page* page = sm_manager_->get_bpm()->fetch_page({fh_->GetFd(), rids_[k].page_no});
                    page->set_page_lsn(lsn);
                    sm_manager_->get_bpm()->unpin_page({fh_->GetFd(), rids_[k].page_no}, true);
                }
                fh_->update_record(rids_[k], new_tuple[k].get()->data, context_);
            }
        }
        else{
            // 冲突
            assert(rids_.size() == new_tuple.size());
            // 恢复索引
            for(size_t i = 0; i < tab_.indexes.size(); ++i) {
                auto& index = tab_.indexes[i];
                auto ih = sm_manager_->ihs_.at(sm_manager_->get_ix_manager()->get_index_name(tab_name_, index.cols)).get();
                for(size_t j = 0; j < rids_.size(); j++){
                    ih->insert_entry(delete_index_key[i][j], rids_[j], context_->txn_);
                }
            }
            AppendToOutputFile("failure\n");
            std::cout << "update faliure." << std::endl;
        }

        for(size_t i = 0; i < tab_.indexes.size(); ++i){
            for(size_t j = 0; j < rids_.size(); j++){
                delete[] delete_index_key[i][j];
                delete[] insert_index_key[i][j];
            }
        }

        return nullptr;
    }

    Rid &rid() override { return _abstract_rid; }
};